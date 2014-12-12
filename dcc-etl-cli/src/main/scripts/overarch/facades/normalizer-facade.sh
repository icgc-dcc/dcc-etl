#!/bin/bash -e
# Facade script around the normalizer
# usage: See call in parent script
# point person: Anthony

# ===========================================================================
# Include dependency
source helpers/utils.sh
source helpers/hdfs.sh
source helpers/cmd_builder.sh
source helpers/constants.sh

# ---------------------------------------------------------------------------
# Sanity checks
ensure_user "dcc_dev"
ensure_pwd "overarch"

# ===========================================================================

project_keys=${1?} && shift # The full list of projects (whether they have SSM data or not)
jar_file=${1?} && shift
nn_server=${1?} && shift
jt_server=${1?} && shift
parent_cluster_output_dir=${1?} && shift

# ===========================================================================
# constants:

main_class="org.icgc.dcc.submission.validation.norm.cli.Main"
logback_file="/etl/conf/logback.xml" # in git
fuse_mount_point="/hdfs/dcc"

concatenator_cluster_output_dir="${parent_cluster_output_dir?}/${concatenator_component?}"
normalizer_cluster_output_dir="${parent_cluster_output_dir?}/${normalizer_component?}"

export HADOOP_USER_NAME=hdfs

# ===========================================================================

function create_hdfs_dir_if_doesnt_exists() {
	dir="${1?}"
	if ! hdfs_is_dir ${dir?}; then
		echo "Creating ${dir?}"
		hadoop fs -mkdir ${dir?}
	else
		echo "${dir?} already exists"
	fi
}

#---------------------------------------------------------------------------

function process() {
    project_key=${1?} && shift
    final_output_file=${1?} && shift
	type=${1?} && shift

    echo
    print_stdout_section_separator
    new_cmd_builder
    
    add_to_cmd "java"
    add_to_cmd "   -Dlogback.configurationFile=${logback_file?}"
    add_to_cmd " -cp ${jar_file?}"
    add_to_cmd "  ${main_class?}"
    add_to_cmd "  ${parent_cluster_output_dir?}"
    add_to_cmd "  \"${project_key?}\""
    add_to_cmd "  ${nn_server?}"
    add_to_cmd "  ${jt_server?}"
    add_to_cmd "  ${type?}"

    cmd=$(build_cmd)
    pretty_print_cmd "${cmd?}"
    eval_cmd "${cmd?}"
}


# ===========================================================================

# Create parent output dir; TODO: have normalizer take care of that
create_hdfs_dir_if_doesnt_exists "${normalizer_cluster_output_dir?}"

# Generate SSM/SGV output for all projects with that data type (parallelized)
echo
pids=""
for type in ssm sgv; do
	for project_key in ${project_keys?}; do
		print_stdout_section_separator

		project_cluster_output_dir="${normalizer_cluster_output_dir?}/${project_key?}"
		echo "project_cluster_output_dir=\"${project_cluster_output_dir?}\""

		# ---------------------------------------------------------------------------
		project_fuse_input_file="${fuse_mount_point?}${concatenator_cluster_output_dir?}/${project_key?}/${type?}_p.txt"
		if [ -f "${project_fuse_input_file?}" ]; then # Only run annotator if there is a p file; TODO: consider filtering out projects without SSM/SGV data at the overarching script level?
		    echo "project_fuse_input_file=\"${project_fuse_input_file?}\""
		    create_hdfs_dir_if_doesnt_exists "${project_cluster_output_dir?}"
			process ${project_key?} ${project_cluster_output_dir?}/${type?}_p.txt ${type?} &
		    pid=$!
		    pids="${pids?} ${pid?}"
		    echo "started ${project_key?} with PID '${pid?}'"
		else
			echo "No ${type?}_p file at \"${project_fuse_input_file?}\", skipping normalization for ${project_key?}"
		fi
	done
done

echo "PIDs: '${pids?}'"
for pid in ${pids?}; do
 echo "waiting for PID '${pid?}'"
 wait ${pid?}
done

# ===========================================================================

