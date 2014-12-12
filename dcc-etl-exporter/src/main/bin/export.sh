#!/bin/bash

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR
nohup ${EXPORTHOMEDIR}/bin/static-export.sh "$@" &
static_pid=$!

${EXPORTHOMEDIR}/bin/dynamic-export.sh "$@"
dynamic_code=$?
wait $static_pid
static_code=$?

if [ $static_code -eq 0 -a $dynamic_code -eq 0 ]; then
    exit 0
else
    exit 1
fi