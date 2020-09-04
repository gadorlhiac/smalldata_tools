import psana
from psmon.plots import Image
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
from psmon import publish
from smalldata_tools.DetObject import DetObject
from smalldata_tools.azimuthalBinning import azimuthalBinning
import numpy as np
import os
import logging
import requests
import socket
import argparse
import sys
import time
import inspect
from mpi4py import MPI

from smalldata_tools.SmallDataUtils import detData

f = '%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=f)
logger = logging.getLogger(__name__)


class MpiWorker(object):
    """This worker will collect events and do whatever
    necessary processing, then send to master"""
    def __init__(self, ds, evnt_lim, detector, rank, var_list, damage_list, r_mask, latency=0.5, event_code=40, evr_use='evr1', plot=False):
        self._ds = ds
        self._evnt_lim = evnt_lim
        self._detector = detector
        self._damage_list = damage_list
        self._var_list = var_list
        self._comm = MPI.COMM_WORLD
        self._rank = self._comm.Get_rank()
        self._r_mask = r_mask
        self._plot = plot
        self._latency = latency
        self._event_code = event_code
        self._evr_use = evr_use
        #publish.init()

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
    def detector(self):
        """Detectors to get data from"""
        return self._detector

    @property
    def comm(self):
        """MPI communicator"""
        return self._comm

    @property
    def plot(self):
        """Whether we should plot detector"""
        return self._plot

    @property
    def latency(self):
        """Max latency allowed between event and results"""
        return self._latency

    @property
    def event_code(self):
        """Event Code to trigger data collection on"""
        return self._event_code

    @property
    def evr_use(self):
        """Evr the detector is running on"""
        return self._evr_use

    def start_run(self):
        """Worker should be incredibly light weight"""
        #logger.debug('Starting worker {0} with dets {1}'.format(self._rank, self.detectors))
        #mask_det = det.mask(188, unbond=True, unbondnbrs=True, status=True,  edges=True, central=True)
        det_names = psana.DetNames()
        print('evr ', det_names)
        evr_det = psana.Detector(self.evr_use)  # Get this from driver
        ped = self.detector.pedestals(188)[0]  # Get this from driver
        gain = self.detector.gain(188)[0]  # Get this from driver
        #bld = psana.Detector('EBeam')
        skipped_events = 0
        for evt_idx, evt in enumerate(self.ds.events()):
            # Don't do anything if the event doesn't have requested event code
            #if self.event_code not in evr_det.eventCodes(evt):
            #     continue
            
            # Compare event timestamp to current time for latency
            ts = evt.get(psana.EventId).time()
            ts = ts[0] + ts[1] / 1e9
            delay = abs(time.time() - ts)

            # If we're falling behind just skip events
            if delay > self.latency:
                continue

            # TODO: Provide options to use ped and gain, use mask
            raw = (self.detector.raw_data(evt) - ped) * gain
            data = self.detector.image(evt, raw)
            az_bins = np.array([np.mean(data[mask]) for mask in self._r_mask])
            self.comm.Send(az_bins, dest=0, tag=self.rank)
            # float32
            #if evt_idx==2:
            #    plt.plot(az_bins)
            #    plt.show()
            #img = Image(0, 'cspad', data)
            #publish.send('image', img)
            #default_data = detData(self.detectors, evt)
            
            # Check for damaged detectors to continue
            #damaged = self.check_damage(self._damage_list, default_data['damage'])
            #if damaged:
            #    continue
            
            #self.comm.Send(data, dest=0, tag=ts)
