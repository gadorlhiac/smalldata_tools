#!/bin/bash

usage()
{
cat << EOF
$(basename "$0"): 
	Script to launch a smalldata_tools run analysis
	
	OPTIONS:
		-h|--help
			Definition of options
		-e|--experiment
			Experiment name (i.e. cxilr6716)
		-r|--run
			Run Number
		-d|--directory
			Full path to directory for output file
		-n|--nevents
			Number of events to analyze
		-q|--queue
			Queue to use on SLURM
		-c|--cores
			Number of cores to be utilized
		-f|--full
			If specified, translate everything
		-D|--default
			If specified, translate only smalldata
                -i|--image
			If specified, translate everything & save area detectors as images
		--norecorder
			If specified, don't use recorder data
                --nparallel
                        Number of processes per node
                --postTrigger
                        Post that primary processing done to elog to seconndary jobs can start
                --interactive
                        Run the process live w/o batch system
EOF

}
#Look for args we expect, for now ignore other args
#Since we can have a mix of flags/args and args do in loop

#Generate smalldata_producer_arp.py args, for now if
#Experiment and Run not specified assume it's being handed
#by the ARP args in the os.environment

POSITIONAL=()
while [[ $# -gt 0 ]]
do
        key="$1"

	case $key in
		-h|--help)
			usage
			exit
			;;
		-q|--queue)
			QUEUE="$2"
			shift
			shift
			;;
		-c|--cores)
			CORES="$2"
			shift
			shift
			;;
        --nparallel)
            TASKS_PER_NODE="$2"
			shift
			shift
			;;
        --account)
            ACCOUNT="$2"
			shift
			shift
			;;
        --reservation)
            RESERVATION="$2"
			shift
			shift
			;;
        --interactive)
            INTERACTIVE=1
            POSITIONAL+=("$1")
			shift
			;;
        --logfile)
            LOGFILE="$2"
            shift
            shift
            ;;
        *)
            POSITIONAL+=("$1")
			shift
			;;                     
	esac
done
set -- "${POSITIONAL[@]}"

DEFQUEUE='psanaq'
if [[ $HOSTNAME == *drp* ]]; then
    DEFQUEUE='anaq'
elif [[ $HOSTNAME == *sdf* ]]; then
    DEFQUEUE='milano'
fi
#Define cores if we don't have them
#Set to 1 by default
CORES=${CORES:=1}
QUEUE=${QUEUE:=$DEFQUEUE}
ACCOUNT=${ACCOUNT:='lcls'}
RESERVATION=${RESERVATION:=''}
#QUEUE=${QUEUE:='anaq'}
# select tasks per node to match the number of cores:
if [[ $QUEUE == *psanaq* ]]; then
    TASKS_PER_NODE=${TASKS_PER_NODE:=12}
elif [[ $QUEUE == *psfeh* ]]; then
    TASKS_PER_NODE=${TASKS_PER_NODE:=16}
elif [[ $QUEUE == *ffb* ]]; then
    TASKS_PER_NODE=${TASKS_PER_NODE:=60}
elif [[ $QUEUE == *milano* ]]; then
    TASKS_PER_NODE=${TASKS_PER_NODE:=128}
else
    TASKS_PER_NODE=${TASKS_PER_NODE:=12}
fi

if [ $TASKS_PER_NODE -gt $CORES ]; then
    TASKS_PER_NODE=$CORES 
fi

#need to export this so that it can be used in the follow-up script even in SLURMx
export MYDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
if [ -v INTERACTIVE ]; then
    $MYDIR/submit_small_data.sh -c $CORES $@
    exit 0
fi
if [ -v LOGFILE ]; then
    sbatch -p $QUEUE --ntasks-per-node $TASKS_PER_NODE --ntasks $CORES --exclusive --output $LOGFILE $MYDIR/submit_small_data.sh $@
    exit 0
fi

if [[ $QUEUE == *milano* ]]; then
    if [[ $ACCOUNT == 'lcls' ]]; then
	sbatch -p $QUEUE --ntasks-per-node $TASKS_PER_NODE --ntasks $CORES --exclusive --account $ACCOUNT --qos preemptable $MYDIR/submit_small_data.sh $@
    else
	sbatch -p $QUEUE --ntasks-per-node $TASKS_PER_NODE --ntasks $CORES --exclusive --account $ACCOUNT $MYDIR/submit_small_data.sh $@
    fi
else
    sbatch -p $QUEUE --ntasks-per-node $TASKS_PER_NODE --ntasks $CORES --exclusive $MYDIR/submit_small_data.sh $@
fi
