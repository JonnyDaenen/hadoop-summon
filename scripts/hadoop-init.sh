#!/bin/sh

# load env vars 
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPTDIR/hadoop-env.sh


# Smoke Test:
hadoop dfsadmin -report

# create user
hadoop fs -mkdir -p /user/$HADOOP_USERNAME

# open new shell
#bash -l
