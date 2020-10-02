from mpi4py import MPI
import sys
import logging
import numpy as np
import zmq
import time
from epics import caput
from threading import Thread
from enum import Enum
from collections import deque
#from shmem_scripts.shmem_data import ShmemData

f = '%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=f)
logger = logging.getLogger(__name__)


class MpiMaster(object):
    def __init__(self, rank, api_port, det_map, psana=True, data_port=8123):
        self._rank = rank
        self._det_map = det_map
        self._psana = psana
        self._comm = MPI.COMM_WORLD
        self._workers = range(self._comm.Get_size())[1:]
        self._running = False
        self._abort = False
        self._queue = deque()
        self._msg_thread = Thread(target=self.start_msg_thread, args=(api_port,))
        self._msg_thread.start()
        self._queue_thread = Thread(target=self.start_queue_thread, args=(data_port,))
        self._queue_thread.start()

    @property
    def rank(self):
        """Master rank (should be 0)"""
        return self._rank

    @property
    def comm(self):
        """Master communicator"""
        return self._comm

    @property
    def workers(self):
        """Workers currently sending"""
        return self._workers

    @property
    def det_map(self):
        """Detector info"""
        return self._det_map

    @property
    def queue(self):
        """Queue for processing data from workers"""
        return self._queue

    @property
    def running(self):
        """Check if master is running"""
        return self._running

    @property
    def abort(self):
        """See if abort has been called"""
        return self._abort

    @abort.setter
    def abort(self, val):
        """Set the abort flag"""
        if isinstance(val, bool):
            self._abort = val

    def start_run(self):
        self._running = True
        while not self._abort:
            status = MPI.Status()
            ready = self.comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            if ready:
                data = np.empty(2, dtype=np.dtype(self.det_map['dtype']))
                self.comm.Recv(data, source=status.Get_source(), tag=MPI.ANY_TAG, status=status)
                self.queue.append(data)
            else:
                # We can use this to perform tasks
                pass

        logger.debug('Abort has been called, terminating mpi run')
        MPI.Finalize()

    def start_msg_thread(self, api_port):
        """The thread runs a PAIR communication and acts as server side,
        this allows for control of the parameters during data aquisition 
        from some client (probably an API for user). Might do subpub if
        we want messages to be handled by workers as well, or master can
        broadcast information
        """
        context = zmq.Context()
        socket = context.socket(zmq.PAIR)
        # TODO: make IP available arg
        socket.bind(''.join(['tcp://*:', str(api_port)]))
        while True:
            message = socket.recv()
            if message == 'abort':
                self.abort = True
                socket.send('aborted')
            else:
                print('Received Message with no definition ', message)

    def start_queue_thread(self, data_port):
        """This thread simply looks for items in the queue and writes
        the data to the appropriate PVs
        """
        if self._psana:
            context = zmq.Context()
            socket = context.socket(zmq.PUB)
            socket.bind(''.join(['tcp://*:', str(data_port)]))
            flags = 0
        sent = 0
        start = time.time()
        while True:
            if len(self.queue) > 0:
                data = self.queue.popleft()
                if self._psana:
                    # Could need this if sending large arrays
                    md = dict(
                        dtype = str(data.dtype),
                        shape = data.shape
                    )
                    socket.send_json(md, flags|zmq.SNDMORE)
                    socket.send(data, flags, copy=False, track=False)
                else:
                    # TODO: These need to be passable 
                    caput('CXI:JTRK:REQ:DIFF_INTENSITY', data[1])
                    caput('CXI:JTRK:REQ:I0', data[0])
                sent += 1
                print('data rate ', sent / (time.time() - start))
                if sent == 1000:
                    sent = 0
                    start = time.time()
