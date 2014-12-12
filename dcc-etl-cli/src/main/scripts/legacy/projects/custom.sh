#!/bin/bash -e

export run_type="custom"
export config_file="***REMOVED***/dcc-etl/conf/etl_dev.yaml"

unset input_projects && declare -A input_projects
input_projects[14]=""
input_projects[13]=""
input_projects[13_ambiguous]=""
input_projects[12]=""
input_projects[11]=""
input_projects[10]=""
input_projects[9]=""
input_projects[8]=""
export input_projects

export expected_count=0

exit 1 # Don't leave as it
