from mpi4py import MPI
import logging
#from shmem_scripts.shmem_data import ShmemData

f = '%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=f)
logger = logging.getLogger(__name__)

class MpiMaster(object):
    def __init__(self, rank):
        self._rank = rank
        self._comm = MPI.COMM_WORLD
        self._workers = []
        self._running = False
        self._abort = False

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
            req = self.comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
            # Do any work here
            data = req.wait()
            logger.debug('Master quired data')

        size = MPI.Get_size()
        MPI.Finalize()

