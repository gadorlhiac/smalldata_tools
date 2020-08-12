import psana
from psmon.plots import Image
from psmon import publish
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


class MpiWorker(object):
    """This worker will collect events and do whatever
    necessary processing, then send to master"""
    def __init__(self, ds, evnt_lim, det_keys, rank, var_list, damage_list):
        self._ds = ds
        self._evnt_lim = evnt_lim
        self._det_keys = det_keys
        self._damage_list = damage_list
        self._var_list = var_list
        self._comm = MPI.COMM_WORLD
        self._rank = rank
        publish.init()

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
    def ds(self):
        """DataSource object"""
        return self._ds

    @property
    def det_keys(self):
        """Detectors to get data from"""
        return self._det_keys

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
        logger.debug('Starting worker {0} with dets {1}'.format(self._rank, self.det_keys.keys()))
        for evt_idx, evt in enumerate(self.ds.events()):
            #print(psana.DetNames())
            data = psana.Detector('DsdCsPad').image(evt)
            img = Image(0, 'cspad', data)
            publish.send('image', img)
            default_data = detData(self._detectors, evt)
            # Check for damaged detectors to continue
            damaged = self.check_damage(self._damage_list, default_data['damage'])
            if damaged:
                continue
            
            # See if we find our special keys in dict two levels deep
            result = self.unpack(self._var_list, default_data, {})
            ts = evt.get(psana.EventId).time()
            result['event_time'] = '%.4f' % (ts[0] + ts[1]/1e9)
            # This is where work for data preparation needs to go
            # we'll use send for py objects and Send for arrays
            #data = np.arange(100, dtype=np.float64)
            print('worker ', data)
            #self.comm.Send(data, dest=0, tag=1)

            if evt_idx == self.evnt_lim:
                logger.debug('We collected our events, exiting')
                MPI.Finalize()
