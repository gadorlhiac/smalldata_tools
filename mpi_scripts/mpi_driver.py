# Local imports
from mpi_worker import MpiWorker
from mpi_master import MpiMaster
from utils import get_r_masks
from mpi4py import MPI
import psana
import yaml
import logging
import re
import argparse

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

f = '%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=f)
logger = logging.getLogger(__name__)

# All the args
parser = argparse.ArgumentParser()
parser.add_argument('--exprun', help='psana experiment/run string (e.g. exp=xppd7114:run=43)', type=str, default='')
#parser.add_argument('--dsname', help='data source name', type=str, default='')
parser.add_argument('--nevts', help='number of events', default=50, type=int)
parser.add_argument('--cfg_file', help='if specified, has information about what metadata to use', type=str, default='default_config.yml')
args = parser.parse_args()

if args.exprun:
    info = re.split('=|:', args.exprun)
    exp = info[1]
    hutch = exp[:3]
    exp_dir = ''.join(['/reg/d/psdm/', hutch, '/', exp, '/xtc/'])
    dsname = ''.join([args.exprun, ':smd:', 'dir=', exp_dir]) 
else:  # Assume we want shared memory for now
    dsname = 'shmem=psana.0:stop=no'
    psana.setOption('psana.calib-dir', '/reg/d/psdm/cxi/cxilv9518/calib/')

# Parse config file to hand to workers
with open(''.join(['mpi_configs/', args.cfg_file])) as f:
    yml_dict = yaml.load(f, Loader=yaml.FullLoader)
    var_list = yml_dict['var_list']
    damage_list = yml_dict['damage_list']
    api_port = yml_dict['api_msg']['port']
    det_map = yml_dict['det_map']
    ipm = yml_dict['ipm']
    evr = yml_dict['evr']

# Can look at ways of automating this later
#if not args.exprun:
#    raise ValueError('You have not provided an experiment')
ds = psana.DataSource(dsname)
detector = psana.Detector(det_map['name'])
gdet = psana.Detector('FEEGasDetEnergy')
ipm = psana.Detector(ipm)
evr = psana.Detector(evr)
r_mask = get_r_masks(det_map['shape'])

if rank == 0:
    master = MpiMaster(rank, api_port, det_map)
    master.start_run()
else:
    worker = MpiWorker(ds, args.nevts, detector, gdet, evr, damage_list, r_mask)
    worker.start_run()
