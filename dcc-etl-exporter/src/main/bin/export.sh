#!/bin/bash
#
# Copyright 2014(c) The Ontario Institute for Cancer Research. All rights reserved.
#
# Description:
#   export data for static and dynamic download in the portal for a given release
#
# Usage:
#   ./export.sh <release_name> <loader_output_directory> <data_types>
#
# Example:
#   ./export.sh test_release /icgc/overarch/test18/0/7/loader clinical,clinicalsample,ssm_open

# Prolog
set -o nounset
set -o errexit

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR

logfile=${EXPORTHOMEDIR}/logs/exporter.ec
touch $logfile
before=`stat -c %Y $logfile`

nohup ${EXPORTHOMEDIR}/bin/static-export.sh "$@" &
static_pid=$!

${EXPORTHOMEDIR}/bin/dynamic-export.sh "$@"
wait $static_pid

after=`stat -c %Y $logfile`

if [ "$before" -eq "$after" ]; then
    exit 0
else
    exit 1
fi