#!/bin/bash -e
# Facade script around the annotator
# usage: annotator-facade.sh <hadoop_fs_url> <job_tracker_url> <working_dir> <project_name_to_be_processed>
# point person: Vitalii

# ===========================================================================
# Include dependency
source helpers/utils.sh
source helpers/cmd_builder.sh
source helpers/constants.sh

# ---------------------------------------------------------------------------
# Sanity checks
ensure_user "dcc_dev"
ensure_pwd "overarch"

# ===========================================================================
etl_dir=${1?} && shift
fs_url=${1?} && shift
job_tracker=${1?} && shift
working_dir=${1?} && shift
project_names=${1}

project_names=$(echo ${project_names})

echo "working_dir=\"${working_dir?}\""
echo "project_names=\"${project_names}\""

# ===========================================================================
# constants:

jar_file="${etl_dir?}/lib/dcc-annotator.jar"
conf_file="${etl_dir?}/conf/annotator.yml"
java_opts="-Xmx4g -Xms4g"

# ===========================================================================

echo
print_stdout_section_separator
new_cmd_builder
add_to_cmd "java"
add_to_cmd "  ${java_opts?}"
add_to_cmd "  -Dhadoop.properties.fs.defaultFS=\"${fs_url?}\""
add_to_cmd "  -Dhadoop.properties.mapred.job.tracker=\"${job_tracker?}\""
add_to_cmd "  -jar ${jar_file?}"
add_to_cmd "  --spring.config.location=${conf_file?}"
add_to_cmd "  --working-dir ${working_dir?}"

if [ -n "${project_names}" ]; then
  add_to_cmd "  --project-names ${project_names}"
fi


cmd=$(build_cmd)
pretty_print_cmd "${cmd?}"
eval_cmd "${cmd?}"

# ===========================================================================
