#!/bin/bash
#
# Copyright 2014(c) The Ontario Institute for Cancer Research. All rights reserved.
#
# Description:
#   Runs the annotator
#
# Usage:
#   ./annotator.sh <--working-dir /path/to/the/dir> [--project-names A,B] [--file-types ssm|sgv|ssm,sgv]
#
# Example:
#   ./annotator.sh --working-dir /tmp/ICGC20 --project-names ALL_US --file-types ssm,sgv

base_dir=$(dirname $0)/..
java_opts="-Xmx4g -Xms4g"

java \
  ${java_opts} \
  -jar ${base_dir}/lib/dcc-etl-annotator.jar "$@" \
  --spring.config.location=${base_dir}/conf/application.yml
