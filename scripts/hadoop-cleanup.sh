#!/bin/sh

# load env vars 
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPTDIR/hadoop-env.sh

# TODO first stop hadoop, then delete

cd $HADOOP_WORKDIR/work

# secure logs
mv `echo hadoop-conf-*`/logs `echo hadoop-conf-*`/logs_conf
cp -r `echo hadoop-2.*`/logs `echo hadoop-conf-*`/logs

# remove files
rm hadoop.tar.gz
rm -rf `echo hadoop-2.*`

# kill job (last thing to do!)
qdel $HADOOP_JOBID

cd -