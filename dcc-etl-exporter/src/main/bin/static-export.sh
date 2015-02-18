#!/bin/bash
#
# Copyright 2014(c) The Ontario Institute for Cancer Research. All rights reserved.
#
# Description:
#   export data for static download in the portal for a given release
#
# Usage:
#   ./static-export.sh <release_name> <loader_output_directory> <data_types>
#
# Example:
#   ./static-export.sh test_release /icgc/overarch/test18/0/7/loader clinical,clinicalsample,ssm_open

# Prolog
set -o nounset
set -o errexit

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR
source ${EXPORTHOMEDIR}/bin/setenv.sh

start_time=`date +%s`

# Export
${EXPORTHOMEDIR}/bin/parallel -r -j ${#types[@]} "${EXPORTHOMEDIR}/pig/bin/pig -l ${EXPORTHOMEDIR}/logs/*.log -4 ${EXPORTHOMEDIR}/conf/log4j.properties ${EXPORTHOMEDIR}/scripts/exporter.py -s -d * -e ${EXPORTHOMEDIR}/scripts -i ${source} -r ${release} -l ${logfile}" ${types[@]}

end_time=`date +%s`
echo Total export time was `expr $end_time - $start_time` s.
