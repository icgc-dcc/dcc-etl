#!/bin/bash -e
# Facade script around the exporter
# usage: See call in parent script
# point person: Jerry
# TODO:
# - Allow specifying the full path for output directories (i.e. not hardcoding /hdfs/dcc/icgc/download/static and /hdfs/dcc/icgc/download/dynamic in the exporter)
# - Read/Write to/from HDFS (should not have to use fuse)

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

job_id=${1?} && shift
parent_cluster_output_dir=${1?} && shift
etl_dir=${1?} && shift
echo "job_id=\"${job_id?}\""
echo "parent_cluster_output_dir=\"${parent_cluster_output_dir?}\""
echo "etl_dir=\"${etl_dir?}\""

# ---------------------------------------------------------------------------

exporter_bin_dir="${etl_dir?}/../dcc-exporter/bin"
echo "exporter_bin_dir=\"${exporter_bin_dir?}\""

echo
# ===========================================================================

# Determine JAVA HOME
java_home="$(dirname $(readlink -m $(which java)))/.."
ls ${java_home?}/bin/java || { echo "ERROR: invalid JAVA_HOME '${java_home?}'"; exit 1; }
echo "java_home=\"${java_home?}\""
echo 

input_dir=${parent_cluster_output_dir?}/${loader_component?}
types=$(hadoop fs -ls -h ${input_dir?} | tail -n +2 | cut -d "/" -f 8)
data_type=$(echo $types | tr ' ' ',')

# Build command
new_cmd_builder
add_to_cmd "JAVA_HOME=${java_home?}"
add_to_cmd "${exporter_bin_dir?}/export.sh"
add_to_cmd "  ${job_id?}" # output directory name
add_to_cmd "  ${input_dir?}" # input data
add_to_cmd "  ${data_type?}" # comma-separated data types to process
cmd=$(build_cmd)
pretty_print_cmd "${cmd?}"
eval_cmd "${cmd?}"

# ===========================================================================

exporter_static_parent_fuse_output_dir="${fuse_mount_point?}/tmp/download/static/${job_id?}" # fs-convention
echo "exporter_static_parent_output_dir=\"${exporter_static_parent_fuse_output_dir?}\""
tree ${exporter_static_parent_fuse_output_dir?}

# ===========================================================================
