#!/bin/bash

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR
JOB_USER=hbase

if [[ $# -ne 1 && $# -ne 2 ]]
then
  echo "Usage: `basename $0` <release name> [data type]"
  echo "Example: `basename $0` r12  # release all data types of release r12
  echo "Example: `basename $0` r12 ssm # release only ssm for release r12
  exit $E_BADARGS
fi

releaseName=$1

#export HBASE_HOME=/usr/lib/hbase
export HADOOP_USER_NAME=${JOB_USER}
export HADOOP_CLASSPATH=”`hbase classpath`:$HADOOP_CLASSPATH”
export PIG_CLASSPATH="`hbase classpath`:${EXPORTHOMEDIR}/lib/dcc-etl-exporter.jar"

if [ $# -eq 2 ]
then
	${EXPORTHOMEDIR}/pig/bin/pig -l ${EXPORTHOMEDIR}/logs/release.log -4 ${EXPORTHOMEDIR}/conf/log4j.properties ${EXPORTHOMEDIR}/scripts/releaser.py -d $2 -r "${releaseName}"
else
	${EXPORTHOMEDIR}/pig/bin/pig -l ${EXPORTHOMEDIR}/logs/release.log -4 ${EXPORTHOMEDIR}/conf/log4j.properties ${EXPORTHOMEDIR}/scripts/releaser.py -r "${releaseName}"
fi
exit $?
