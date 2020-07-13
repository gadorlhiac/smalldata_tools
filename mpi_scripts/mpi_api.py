import yaml
import zmq
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--cfg_file', help='if specified, has information about what metadata to use', type=str, default='default_config.yml')
args = parser.parse_args()

with open(''.join(['mpi_configs/', args.cfg_file])) as f:
    yml_dict = yaml.load(f, Loader=yaml.FullLoader)
    api_port = yml_dict['api_msg']['port']

context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect(['tcp://localhost:', api_port])

def abort():
    """Abort MPI run"""
    socket.send('abort')

def pause():
    """Pause MPI run"""
    socket.send('pause')