#!/bin/bash

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR

JOB_USER=downloader
E_BADARGS=65

if [[ $# -ne 2 && $# -ne 3 ]]
then
  echo "Usage: `basename $0` <release to upload to> <Directory containing json directories> [<comma separated list of data types>]"
  echo "Example: `basename $0` r12 /icgc/etl/dcc-release-r--prod-06d-23-1 # process all data types"
  echo "Example: `basename $0` r12 /icgc/etl/dcc-release-r--prod-06d-23-1 ssm,meth # process ssm and meth only"
  exit $E_BADARGS
fi

release=$1
source=$2

declare -a default="ssm_open,ssm_control,pexp,mirna,meth,jcn,exp,clinical,clinicalsample,cnsm,stsm,sgv"
if [ $# -eq 3 ]
then
  default=$3
fi

IFS=',' read -a types <<< "$default"

#export HBASE_HOME=/usr/lib/hbase
export HADOOP_USER_NAME=${JOB_USER}
export HADOOP_CLASSPATH=”`hbase classpath`:$HADOOP_CLASSPATH”
export PIG_CLASSPATH="`hbase classpath`:${EXPORTHOMEDIR}/lib/dcc-etl-exporter.jar"

start_time=`date +%s`

${EXPORTHOMEDIR}/bin/parallel -r -j ${#types[@]} "${EXPORTHOMEDIR}/pig/bin/pig -l ${EXPORTHOMEDIR}/logs/*.log -4 ${EXPORTHOMEDIR}/conf/log4j.properties ${EXPORTHOMEDIR}/scripts/exporter.py -d * -e ${EXPORTHOMEDIR}/scripts -i ${source} -r upload -u ${release}" ${types[@]}

end_time=`date +%s`
echo Total upload time was `expr $end_time - $start_time` s.
