import numpy as np
import logging
import socket
import psana

from smalldata_tools.BaseSmallDataAna_psana import BaseSmallDataAna_psana
from smalldata_tools.DetObject_lcls2 import DetObject_lcls2
from smalldata_tools.utilities import printR

from mpi4py import MPI
import h5py
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SmallDataAna_psana_lcls2(BaseSmallDataAna_psana):
    def __init__(self, expname='', run=-1, dirname='', filename='', plotWith='matplotlib'):
        super().__init__(expname=expname, run=run, dirname=dirname, filename=filename, plotWith=plotWith)
        
        xtcdirname = None
        hostname = socket.gethostname()
        if hostname.find('drp-srcf')>=0:
            xtcdirname = f'/cds/data/drpsrcf/{self.hutch.lower()}/{expname}/xtc'
        self.ds = psana.DataSource(exp=expname, run=run, dir=xtcdirname)
        self.run = next(self.ds.runs())
        return
    
    
    def addDetInfo(self, detname='None', common_mode=None):
        if detname=='None':
            print('Please pass a detector name')
            # aliases = self._getDetName()
            # if len(aliases)==1:
            #     detname = aliases[0]
            # else:
            #     detsString='detectors in event: \n'
            #     for alias in aliases:
            #         detsString+=alias+', '
            #     print(detsString)
            #     detname = raw_input("Select detector to get detector info for?:\n")

        detnameDict=None
        if isinstance(detname, dict):
            detnameDict = detname
            if 'common_mode' in detnameDict.keys() and common_mode is not None:
                common_mode = detnameDict['common_mode']
            detname = detnameDict['source']

        # only do this if information is not in object yet
        if detname in self.__dict__.keys():
            if self.__dict__[detname].common_mode==common_mode and detnameDict is not None:
                return detname
            elif common_mode is None:
                return detname
            else:
                printR(rank, f'Redefine detector object with different common mode:'\
                       '{common_mode} instead of {self.__dict__[detname].common_mode)}')
        
        printR(rank, 'Try to make psana Detector with: %s'%detname)
        
        det = DetObject_lcls2(detname, self.run, name=detname, common_mode=common_mode)
        self.__dict__[detname] = det
        
        if (detname+'_pedestals') not in self.__dict__.keys(): # shouldnt it be 'not in ...'?
            return detname
        
        self.__dict__[detname+'_pedestals'] = det.ped
        self.__dict__[detname+'_rms'] = det.rms
        self.__dict__[detname+'_x'] = det.x
        self.__dict__[detname+'_y'] = det.y
        self.__dict__[detname+'_ix'] = det.ix
        self.__dict__[detname+'_iy'] = det.iy

        return detname
    
    
    def AvImage(self, 
                detname='None', 
                numEvts=100, 
                thresADU=0., 
                thresRms=0., 
                useFilter=None, 
                nSkip=0,minIpm=-1., 
                common_mode=None, 
                std=False, 
                median=False, 
                printFid=False,
                useMask=True, 
                uniform=False, 
                returnEnv=False):
        
        if not isinstance(detname, str):
            print('Please give parameter name unless specifying arguments in right order. detname is first')
            return
        
        self.addDetInfo(detname=detname, common_mode=common_mode)
        
        return
