#!/bin/bash

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR
source ${EXPORTHOMEDIR}/bin/setenv.sh

start_time=`date +%s`

# Export
${EXPORTHOMEDIR}/bin/parallel -r -j ${#types[@]} "${EXPORTHOMEDIR}/pig/bin/pig -l ${EXPORTHOMEDIR}/logs/*.log -4 ${EXPORTHOMEDIR}/conf/log4j.properties ${EXPORTHOMEDIR}/scripts/exporter.py -s -d * -e ${EXPORTHOMEDIR}/scripts -i ${source} -r ${release}" ${types[@]}

end_time=`date +%s`
echo Total export time was `expr $end_time - $start_time` s.
