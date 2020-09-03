from mpi4py import MPI
import sys
import logging
import numpy as np
import zmq
from threading import Thread
from enum import Enum
from collections import deque
#from shmem_scripts.shmem_data import ShmemData

f = '%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=f)
logger = logging.getLogger(__name__)


class MpiMaster(object):
    def __init__(self, rank, api_port, det_map):
        self._rank = rank
        self._det_map = det_map
        self._comm = MPI.COMM_WORLD
        self._workers = []
        self._running = False
        self._abort = False
        self._queue = deque()
        self._msg_thread = Thread(target=self.start_msg_thread, args=(api_port,))
        self._msg_thread.start()
        self._queue_thread = Thread(target=self.start_queue_thread)
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
                data = np.empty(self.det_map['bins'], dtype=np.dtype(self.det_map['dtype']))
                self.comm.Recv(data, source=status.Get_source(), tag=MPI.ANY_TAG, status=status)
                self.queue.append(data)
            else:
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
        socket.bind(''.join(['tcp://*:', str(api_port)]))
        while True:
            message = socket.recv()
            if message == 'abort':
                self.abort = True
                socket.send('aborted')
            else:
                print('Received Message with no definition ', message)

    def start_queue_thread(self):
        """This thread simply looks for items in the queue and writes
        the data to the appropriate PVs
        """
        while True:
            if len(self.queue) > 0:
                print('we have an item in the queue ', self.queue.popleft())
