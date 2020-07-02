import psana
import numpy as np
import os
import logging
import requests
import socket
import argparse
import sys
import time

# Local imports
from mpidata import mpidata 
from smalldata_tools.DetObject import DetObject
from smalldata_tools.SmallDataUtils import *
from smalldata_tools.utilities import checkDet, printMsg
from smalldata_tools.SmallDataDefaultDetector import *

from mpi4py import MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# A lot of this is boilerplate, consider moving to utils
WS_URL = 'https://pswww.slac.stanford.edu/ws/lgbk'
ACTIVE_EXP_URL = '/lgbk/ws/activeexperiment_for_instrument_station'
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

VAR_LIST = [
    'event_time',
    'ipm2_dg2__sum',
    'sc2slit_s',
    'lightStatus__laser',
    'lightStatus__xray'
]

HUTCHES = [
    'amo',
    'sxr',
    'xpp',
    'xcs',
    'mfx',
    'cxi',
    'mec'
]

class ShmemWorker(object):
    def __init__(self, exp_run=None, evnt_lim=50):
        self._exp_run = exp_run
        self._evnt_lim = evnt_lim
        self._hutch = 'xcs'#exp_run[4:6]
        self._run = int(exp_run.split('=')[-1])
        #self._dsname = ''.join([exp_run, ':smd'])
        self._dsname = 'shmem=psana.0:stop=no'
        self._ds = psana.DataSource(self.dsname)
        self._default_dets = defaultDetectors(self._hutch)

    @property
    def exp_run(self):
        """Experiment/run string"""
        return self._exp_run

    @property
    def evnt_lim(self):
        """Number of events for worker to process"""
        return self._evnt_lim

    @property
    def hutch(self):
        """Parse Hutch, check that it's a supported hutch"""
        if self._hutch not in HUTCHES:
            logger.debug('Hutch {0} is not supported'.format(self._hutch))
        
        return self._hutch

    @property
    def run(self):
        """Get run number"""
        return self._run

    @property
    def dsname(self):
        """Generate data source name"""
        return self._dsname

    @property
    def ds(self):
        """Instantiate DataSource object"""
        return self._ds

    @property
    def default_dets(self):
        """This is shared among workers, maybe pull out"""
        return self._default_dets

    def run_worker(self):
        """Worker should be incredibly light weight"""
        logger.debug('Starting worker {0} with dets {1}'.format(rank, self.default_dets))
        for evt_idx, evt in enumerate(self.ds.events()):
            default_data = detData(self.default_dets, evt)
            logger.debug('Got data {0}'.format(default_data))

            if evt_idx == self.evnt_lim:
                logger.debug('We collected our events, exiting')
                MPI.Finalize()

