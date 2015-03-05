#!/bin/bash
#
# Copyright 2015(c) The Ontario Institute for Cancer Research. All rights reserved.
#
# Description:
#   Runs db-importer
#
# Usage:
#   ./db-importer.sh --config /path/to/config/file [--collections projects,genes,cgc,go,pathways]
#
# Example:
#   ./db-importer.sh --config ***REMOVED***/dcc-etl/conf/etl_dev.yaml --collection projects,genes

base_dir=$(dirname $0)/..
java_opts="-Xmx4g -Xms4g"

java \
  ${java_opts} \
-jar ${base_dir}/lib/dcc-etl-db-importer.jar "$@" \
--config=${base_dir}/conf/config.yaml
