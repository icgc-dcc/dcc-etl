#!/bin/bash
#
# Copyright 2014(c) The Ontario Institute for Cancer Research. All rights reserved.
#
# Description:
#   Bind-mounts a specified Docker host directory and runs a command inside a CDH 4.1.2 environment.
#   The docker image `icgcdcc/cdh` is assumed to exist on the host as defined in 
#   https://github.com/icgc-dcc/dcc-docker/blob/master/cdh/Dockerfile. The $CWD will be set to <host_directory>
#   and HDFS FUSE mounted at `/hdfs` before executing the <command>.
#
# Usage:
#   ./cdh-docker.sh <host_directory> <command>
#
# Example:
#   ./cdh-docker.sh ~/docker/app/ "ls /hdfs"

# Prolog
set -o nounset
set -o errexit

# Configuration
readonly base=$1
readonly execute=$2
readonly image=icgcdcc/cdh
readonly mount=$base:/app
readonly command="hadoop-fuse-dfs ro dfs://***REMOVED***:8020 /hdfs/dcc &> /dev/null ; cd /app ; $execute" # TODO: get NN from config
readonly etl_dir=***REMOVED***/dcc-etl

# Pull/ Update Docker image
docker pull $image

# Execute $command in the Docker image
docker run \
 --privileged=true -ti --rm \
 \
 -v $mount \
 -v $etl_dir:/etl \
 -v /tmp:/tmp \
 \
 -v ***REMOVED***/dcc-exporter/:/dcc-exporter \
 -v ***REMOVED***/dcc-exporter/hadoop/conf:/etc/hadoop/conf \
 -v ***REMOVED***/dcc-exporter/hbase/conf:/etc/hbase/conf \
 \
 -v /nfs/hadoop/workspace/backups/dcc-etl/dcc-etl-indexer/:/nfs/hadoop/workspace/backups/dcc-etl/dcc-etl-indexer/ \
 \
 -e SUDO_USER=${SUDO_USER:="UNKNOWN"} \
 -e USER=$USER \
 -e HADOOP_USER_NAME="hdfs" \
 -e etl_dir=$etl_dir \
 \
 $image \
 /bin/bash -c \
 "$command"

