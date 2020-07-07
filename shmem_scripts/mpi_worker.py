import psana
import numpy as np
import os
import logging
import requests
import socket
import argparse
import sys
import time

from mpi4py import MPI

from smalldata_tools.SmallDataUtils import detData

f = '%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=f)
logger = logging.getLogger(__name__)

# Defaults.  These should be passed in by some means,
# like a json/yaml file perhaps.  This will change based on
# what we're trying to do for which hutch, etc...

# Vars we're interested in passing
VAR_LIST = [
    'event_time',
    'ipm2_dg2__sum',
    'sc2slit_s',
    'lightStatus__laser',
    'lightStatus__xray'
]

# This is a list of devices where if damage is 0
# we automatically skip that event.
DAMAGE_LIST = [
    'ipm2',
    'imp5',
    'evr0',
    'tt',
    'enc' 
]

class MpiWorker(object):
    """This worker will collect events and do whatever
    necessary processing, then send to master"""
    def __init__(self, dsname, evnt_lim, detectors, rank, var_list=VAR_LIST, damage_list=DAMAGE_LIST):
        self._dsname = dsname
        self._ds = psana.DataSource(self._dsname)
        self._evnt_lim = evnt_lim
        self._detectors = detectors
        self._damage_list = damage_list
        self._var_list = var_list
        self._comm = MPI.COMM_WORLD
        self._rank = rank

    @property
    def rank(self):
        """Worker ID"""
        return self._rank

    @property
    def var_list(self):
        """List of variables we want to send"""
        return self._var_list

    @property
    def evnt_lim(self):
        """Number of events for worker to process"""
        return self._evnt_lim

    @property
    def dsname(self):
        """Instantiate DataSource object"""
        return self._dsname

    @property
    def detectors(self):
        """Detectors to get data from"""
        return self._detectors

    @property
    def comm(self):
        """MPI communicator"""
        return self._comm

    @staticmethod
    def unpack(l, d, result):
        """Unpack det data dict for keys we want"""
        # Not 100% thread safe, not sure it needs to be, 
        # method can be moved, but should look at caching results
        for k, v in d.items():
            if isinstance(v, dict):
                for k1, v1 in v.items():
                    if isinstance(v1, dict):
                        # Could make this recursive and we don't care, but
                        # if someone nests 1000 levels deep we're in trouble
                        logger.debug('Hit third level nesting, continue')
                        continue
                    new_key = ''.join([k, '__', k1])
                    if new_key in l:
                        result[new_key] = v1
            elif k in l:
                result[k] = v
            
        return result

    @staticmethod
    def check_damage(l, damage_dict):
        """Check if a required detector is damaged"""
        for det in l:
            try:
                if damage_dict[det] is 0:
                    return True
            except:
                pass

    def start_run(self):
        """Worker should be incredibly light weight"""
        logger.debug('Starting worker {0} with dets {1}'.format(self._rank, self.default_dets))
        for evt_idx, evt in enumerate(self.ds.events()):
            default_data = detData(self._detectors, evt)
            
            # Check for damaged detectors to continue
            damaged = check_damage(self._damage_list, default_data['damage'])
            if damaged:
                continue
            
            # See if we find our special keys in dict two levels deep
            result = unpack(self._var_list, default_data, {})
            req = self.comm.isend(result, dest=0, tag=self.rank)
            req.wait()

            if evt_idx == self.evnt_lim:
                logger.debug('We collected our events, exiting')

