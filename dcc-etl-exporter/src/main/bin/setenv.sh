#!/bin/bash

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR

JOB_USER=downloader
E_BADARGS=65

if [[ $# -ne 2 && $# -ne 3 ]]
then
  echo "Usage: `basename $0` <release name> <Directory containing json directories> [<comma separated list of data types>]"
  echo "Example: `basename $0` r12 /icgc/etl/dcc-release-r--prod-06d-23-1 # process all data types"
  echo "Example: `basename $0` r12 /icgc/etl/dcc-release-r--prod-06d-23-1 ssm,meth # process ssm and meth only"
  exit $E_BADARGS
fi

release=$1
source=$2

declare -a datatypes="ssm_open,ssm_controlled,sgv_controlled,pexp,mirna_seq,meth_seq,meth_array,jcn,exp_seq,exp_array,clinical,clinicalsample,cnsm,stsm"
if [ $# -eq 3 ]
then
  datatypes=$3
fi

IFS=',' read -a types <<< "$datatypes"

#export HBASE_HOME=/usr/lib/hbase
export HADOOP_USER_NAME=${JOB_USER}
#export HADOOP_CLASSPATH="`/usr/lib/hbase/bin/hbase classpath`:$HADOOP_CLASSPATH"
export HBASE_CONF_DIR="${EXPORTHOMEDIR}/conf/hbase"
export PIG_USER_CLASSPATH_FIRST=true
export PIG_CLASSPATH="${HBASE_CONF_DIR}:${EXPORTHOMEDIR}/lib/dcc-etl-exporter.jar:`/usr/lib/hbase/bin/hbase classpath`"

#logging
umask 002
export PIG_OPTS=-Dpython.verbose=warning