#!/bin/bash -e
# Facade script around the pruner
# usage: See call in parent script
# point person: Sepehr

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
projects_json_file=${1?} && shift
jar_file=${1?} && shift
dictionary_file=${1?} && shift
config_file=${1?} && shift
parent_cluster_output_dir=${1?} && shift

echo "projects_json_file=\"${projects_json_file?}\""
echo "jar_file=\"${jar_file?}\""
echo "dictionary_file=\"${dictionary_file?}\""
echo "config_file=\"${config_file?}\""
echo "parent_cluster_output_dir=\"${parent_cluster_output_dir?}\""

# ===========================================================================
# constants:

main_class="org.icgc.dcc.etl.pruner.PrunerMain"
logback_file="/etl/conf/logback.xml" # in git

concatenator_cluster_output_dir="${parent_cluster_output_dir?}/${concatenator_component?}"

# ===========================================================================

echo
print_stdout_section_separator
new_cmd_builder
add_to_cmd "java"
add_to_cmd "  -Xmx4g -Xms4g"
add_to_cmd "   -Dlogback.configurationFile=${logback_file?}"
add_to_cmd "  -cp ${jar_file?}"
add_to_cmd "  ${main_class?}"
add_to_cmd "  -i ${concatenator_cluster_output_dir?}"
add_to_cmd "  -p ${projects_json_file?}"
add_to_cmd "  -d ${dictionary_file?}"
add_to_cmd "  -c ${config_file?}"
# remove orphaned data from files. comment out for reports only.
add_to_cmd "  -r"
cmd=$(build_cmd)
pretty_print_cmd "${cmd?}"
eval_cmd "${cmd?}"

# ===========================================================================

