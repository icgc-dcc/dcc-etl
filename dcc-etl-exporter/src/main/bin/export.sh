#!/bin/bash

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