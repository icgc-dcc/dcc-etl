#!/bin/bash -e
# Prints release stats on the supplied dbname
#  Usage: ./etl-stats-mongo.sh <dbname>

if [ -z "$1" ] 
  then echo "No mongo dbname supplied"; exit 1;
fi

db_name=${1?}
passwd_file="***REMOVED***"
dcc_username="***REMOVED***"
dcc_passwd=$(cat ${passwd_file?} | awk -F'=' '/dev\.'${dcc_username?}'/{print $2}')
mongo hmongodb-dev.res --eval "var dbName='${db_name?}', dccusername='${dcc_username?}', dccpasswd='${dcc_passwd?}'"  etl-stats-mongo.js
