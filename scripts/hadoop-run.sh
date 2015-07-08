#!/bin/sh

# this auto-script is supposed to be run on the master node


# load env vars 
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPTDIR/hadoop-env.sh

# go to working dir
cd $HADOOP_WORKDIR

# init
source $SCRIPTDIR/hadoop-init.sh

# run scripts
source $HADOOP_RUNSCRIPT

# kill
source $SCRIPTDIR/hadoop-cleanup.sh

