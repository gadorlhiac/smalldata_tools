# Local imports
from mpi_worker import MpiWorker
from mpi_master import MpiMaster
from utils import get_r_masks
from smalldata_tools.SmallDataUtils import defaultDetectors
# Import mpi, check that we have enough cores
from mpi4py import MPI
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
assert size > 1, 'At least 2 MPI ranks required'
import psana
import yaml
import logging
f = '%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=f)
logger = logging.getLogger(__name__)

# All the args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--exprun', help='psana experiment/run string (e.g. exp=xppd7114:run=43)', type=str, default='')
parser.add_argument('--dsname', help='data source name', type=str, default='')
parser.add_argument('--nevts', help='number of events', default=50, type=int)
parser.add_argument('--cfg_file', help='if specified, has information about what metadata to use', type=str, default='default_config.yml')
args = parser.parse_args()

# Define data source name
dsname = args.dsname
if not dsname:
    if 'shmem' == args.exprun:
        dsname = 'shmem=psana.0:stop=no'
    elif 'shmem' in args.exprun:
        dsname = ''.join([args.exprun, ':smd'])
    else:
        raise ValueError('Data source name could not be determined')

# Parse config file to hand to workers
with open(''.join(['mpi_configs/', args.cfg_file])) as f:
    yml_dict = yaml.load(f, Loader=yaml.FullLoader)
    var_list = yml_dict['var_list']
    damage_list = yml_dict['damage_list']
    api_port = yml_dict['api_msg']['port']
    det_map = yml_dict['det_map']

# Can look at ways of automating this later
#if not args.exprun:
#    raise ValueError('You have not provided an experiment')
hutch = 'cxi'#args.exprun.split('exp=')[1][:3]
dsname = 'shmem=psana.0:stop=no'
psana.setOption('psana.calib-dir', '/reg/d/psdm/cxi/cxic00318/calib/')
ds = psana.DataSource(dsname)
detector = psana.Detector(det_map['name'])
r_mask = get_r_masks(det_map['shape'])
#detectors = defaultDetectors(hutch)
if rank == 0:
    master = MpiMaster(rank, api_port, det_map)
    master.start_run()
else:
    worker = MpiWorker(ds, args.nevts, detector, rank, var_list, damage_list, r_mask)
    worker.start_run()
