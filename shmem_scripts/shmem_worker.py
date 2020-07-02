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

parser = argparse.ArgumentParser()
# First iteration, we require exprun
parser.add_argument('--exprun', help='psana experiment/run string (e.g. exp=xppd7114:run=43)')
parser.add_argument('--nevts', help='number of events, all events=0', default=-1, type=int)
args = parser.parse_args()


def get_exp_ds_info(exp_run):
    """Parse the exp_run arg to get DataSource object,
    hutch name and run number
    """
    if not exp_run:
        raise ValueError('You have not provided an experiment/run')
    
    hutch = exp_run[4:6]
    if hutch not in HUTCHES:
        logger.debug('{0} is not in list of hutches, exiting'.format(hutch))
        sys.exit()

    run = int(exp_run.split('=')[-1])
    dsname = ''.join([exp_run, ':smd'])

    ds = psana.DataSource(dsname)

    return ds, hutch, run

def run_worker(ds, hutch, run, events):
    """Worker should be incredibly light weight"""
    default_dets = defaultDetectors(hutch)
    logger.debug('Starting worker {0} with dets {1}'.format(rank, default_dets))
    for evt_idx, evt in enumerate(ds.events()):
        default_data = detData(default_dets, evt)
        logger.debug('Got data {0}'.format(default_data))