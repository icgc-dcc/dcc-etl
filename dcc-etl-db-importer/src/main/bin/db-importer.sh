#!/bin/bash
#
# Copyright 2015(c) The Ontario Institute for Cancer Research. All rights reserved.
#
# Description:
#   Runs db-importer
#
# Usage:
#   ./db-importer.sh [--collections PROJECTS, CGC, GO, PATHWAYS, GENES]
#
# Example:
#   ./db-importer.sh --collections PROJECTS, GENES

base_dir=$(dirname $0)/..
java_opts="-Xmx4g -Xms4g"

java \
	${java_opts} \
	-Dlogback.configurationFile=${base_dir}/conf/logback.xml \
	-jar ${base_dir}/lib/dcc-etl-db-importer.jar --config ${base_dir}/conf/config.yaml "$@"
