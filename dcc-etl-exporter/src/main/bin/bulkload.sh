#!/bin/bash
#
# Copyright 2014(c) The Ontario Institute for Cancer Research. All rights reserved.
#
# Description:
#   bulkload HFile to HBase for a release
#
# Usage:
#   ./bulkload.sh <release_name> <loader_output_directory> <data_types>
#
# Example:
#   ./bulkload.sh test_release /icgc/overarch/test18/0/7/loader clinical,clinicalsample,ssm_open
 
# Prolog
set -o nounset
set -o errexit

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR
source ${EXPORTHOMEDIR}/bin/setenv.sh

start_time=`date +%s`

# Bulkload
${EXPORTHOMEDIR}/pig/bin/pig -l ${EXPORTHOMEDIR}/logs/bulkloader.log -4 ${EXPORTHOMEDIR}/conf/log4j.properties ${EXPORTHOMEDIR}/scripts/bulkloader.py -e ${EXPORTHOMEDIR}/scripts -r ${release} -d $datatypes

end_time=`date +%s`
echo Total export time was `expr $end_time - $start_time` s.