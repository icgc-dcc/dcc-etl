#!/bin/bash -e

source helpers/utils.sh
source helpers/constants.sh

# ===========================================================================

etl_dir=${1?} && shift
release_name=${1?} && shift
patch_number=${1?} && shift
run_type=${1?} && shift

# ===========================================================================

function set_latest_run_number() {
    dir=${1?} && shift
    run_type=${1?} && shift
	echo "Getting latest run number from ${dir?} ([$(list_content ${dir?})])"
	run_number=$(get_latest_number ${dir?})
	if [ "${run_type?}" == "${NEW_RUN_MODE?}" ]; then
		run_number=$[run_number+1]
	fi
	export run_number
}

function set_latest_attempt_number() {
    dir=${1?} && shift
	echo "Getting latest attempt number from ${dir?} ([$(list_content ${dir?})])"
	attempt_number=$(get_latest_number ${dir?})
	attempt_number=$[attempt_number+1] # Systematically increment (no notion of "rerun" for an attempt)
	export attempt_number
}

# ===========================================================================

jobs_output_dir="${etl_dir?}/${jobs_dir_name?}"

releases_jobs_output_dir="${jobs_output_dir?}/${releases_dir_name?}"
create_dir_if_doesnt_exist ${releases_jobs_output_dir?}
chmod 777 -R ${releases_jobs_output_dir?} 2>&- || :

release_jobs_output_dir="${releases_jobs_output_dir?}/${release_name?}"
create_dir_if_doesnt_exist ${release_jobs_output_dir?}
chmod 777 -R ${release_jobs_output_dir?} 2>&- || :

patches_jobs_output_dir="${release_jobs_output_dir?}/${patches_dir_name?}"
create_dir_if_doesnt_exist ${patches_jobs_output_dir?}
chmod 777 -R ${patches_jobs_output_dir?} 2>&- || :

patch_jobs_output_dir="${patches_jobs_output_dir?}/${patch_number?}"
create_dir_if_doesnt_exist ${patch_jobs_output_dir?}
chmod 777 -R ${patch_jobs_output_dir?} 2>&- || :

runs_jobs_output_dir="${patch_jobs_output_dir?}/${runs_dir_name?}"
create_dir_if_doesnt_exist ${runs_jobs_output_dir?}
chmod 777 -R ${runs_jobs_output_dir?} 2>&- || :

set_latest_run_number ${runs_jobs_output_dir?} ${run_type?}
echo "run_number=\"${run_number?}\""

run_jobs_output_dir="${runs_jobs_output_dir?}/${run_number?}"
create_dir_if_doesnt_exist ${run_jobs_output_dir?}
chmod 777 -R ${run_jobs_output_dir?} 2>&- || :

jobs_output_dir="${run_jobs_output_dir?}/${attempts_dir_name?}"
create_dir_if_doesnt_exist ${jobs_output_dir?}
chmod 777 -R ${jobs_output_dir?} 2>&- || :

set_latest_attempt_number ${jobs_output_dir?}
echo "attempt_number=\"${attempt_number?}\""

export attempt_output_dir="${jobs_output_dir?}/${attempt_number?}"
echo "Creating dir: '${attempt_output_dir?}'"
mkdir ${attempt_output_dir?} # Cannot exist already by design
mkdir ${attempt_output_dir?}/logs

export run_number
export attempt_number

# ===========================================================================

