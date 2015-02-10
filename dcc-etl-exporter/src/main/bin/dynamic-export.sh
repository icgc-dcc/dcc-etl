#!/bin/bash

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR
source ${EXPORTHOMEDIR}/bin/setenv.sh

start_time=`date +%s`

# Extract
${EXPORTHOMEDIR}/bin/parallel -r -j ${#types[@]} "pig -l ${EXPORTHOMEDIR}/logs/*.log -4 ${EXPORTHOMEDIR}/conf/log4j.properties ${EXPORTHOMEDIR}/scripts/exporter.py -b -d * -e ${EXPORTHOMEDIR}/scripts -i ${source} -r ${release}" ${types[@]}

# Bulkload
pig -l ${EXPORTHOMEDIR}/logs/bulkloader.log -4 ${EXPORTHOMEDIR}/conf/log4j.properties ${EXPORTHOMEDIR}/scripts/bulkloader.py -e ${EXPORTHOMEDIR}/scripts -r ${release} -d $datatypes

end_time=`date +%s`
echo Total export time was `expr $end_time - $start_time` s.
