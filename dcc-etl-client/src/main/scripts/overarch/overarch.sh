#!/bin/bash -e
#
# Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
#
# This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
# SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# DCC-1614
#
# Runs the full ETL component with pre-processing
# usage: ./overarch.sh /path/to/etl/dir my_release_prefix my_release_number /local/path/to/default/data /path/to/projects/file/to/source /path/to/jar/file /path/to/config/file /path/to/dictionary/file components_to_run
# example: ./overarch.sh ***REMOVED***/dcc-etl ICGC 16 /hdfs/dcc/icgc/submission/ICGC16 ***REMOVED***/dcc-etl/lib/dcc-etl.jar ./conf/small.json conf/etl_prod.yaml conf/0.8d.json concatenator-annotator

# ===========================================================================
cd $(dirname "$0")

function timestamp() {
 date '+%y/%m/%d-%H:%M:%S'
}
timestamp
whoami
pwd
git rev-parse HEAD
printf '=%.0s' {1..75} && echo
hash hbase # HACK

# ---------------------------------------------------------------------------
# Include dependencies
source helpers/utils.sh
source helpers/cmd_builder.sh
source helpers/constants.sh

ensure_user "dcc_dev"
ensure_pwd "overarch"
export HADOOP_USER_NAME=hdfs

# ---------------------------------------------------------------------------
# Parse arguments
etl_dir=${1?} && shift
release_prefix=${1?} && shift
release_number=${1?} && shift
patch_number=${1?} && shift
default_parent_data_dir=${1?} && shift # /icgc/submission/ICGC16 for instance
projects_json_file=${1?} && shift
etl_jar_file=${1?} && shift
config_file=${1?} && shift
dictionary_file=${1?} && shift
codelists_file=${1?} && shift
components_to_run=${1?} && shift # ordered components using "printing"-like syntax, for instance "-8" (1 to 8) or "1,3-" (all but 2) or "1,2,4-7,10" (1,2,4,5,6,7,10); use components names rather than numbers
index_type=$1

# ---------------------------------------------------------------------------
# Utils functions

function register_new_component() {
	component=${1?}
	printf '=%.0s' {1..75}
	echo
	echo "component=\"${component?}\""
	echo
	current_component_fuse_output_dir="${run_fuse_output_dir?}/${component?}"
	component_log_file="${attempt_output_dir?}/logs/${component?}.log"
}

function eval_cmd_if_component_to_run() {
  cmd=${1?} && shift
  components_to_run=${1?} && shift
  component=${1?} && shift
  log_file=${1?} && shift

  pretty_print_cmd "${cmd?}"

  if $(is_component_to_run "${components_to_run?}" "${component?}"); then
    start=$(date +%s)
    echo "Evaluating '${component?}'"
    echo ${log_file?}
    eval_cmd "${cmd?}" 2>&1 | tee ${log_file?}
    declare -i code=${PIPESTATUS[0]}
    end=$(date +%s)
    runtime=$((end-start))
    duration="$(($runtime / 60)) minutes and $(($runtime % 60)) seconds"
    if [ ${code?} != 0 ]; then
      email ${config_file?} "failed at the '${component?}' level after ${duration}: '${code?}'"
      echo "ERROR: '${code?}'"
      exit ${code?}
    else
      email ${config_file?} "${component?} finished in ${duration} with exit code '${code?}'"
    fi
    echo "elapsed time: $[end-start] ($(timestamp))"
  else
    echo "Skipping '${component?}'"
  fi
  
  if [ "${component?}" == "${concatenator_component?}" -o "${component?}" == "${normalizer_component?}" -o "${component?}" == "${annotator_component?}" ]; then
    ls -d ${current_component_fuse_output_dir?} || { echo "ERROR"; exit 1; }
      tree ${current_component_fuse_output_dir?}
  fi
}

# ===========================================================================
# Init variables / sanity checks

first_component="${concatenator_component?}"
release_name="${release_prefix?}${release_number?}"
overarch_fuse_output_dir="${fuse_mount_point?}${overarch_cluster_output_dir?}"

[[ ${patch_number?} =~ ^[0-9]+$ ]] || { echo "ERROR: patch number is expected to be a positive integer, instead got: '${patch_number?}'"; exit 1; }

tmp=$(echo "${components_to_run?}" | awk '/^-/ || /^'"${first_component?}"'/') && if [ "${components_to_run?}" == "all" -o -n "${tmp?}" ]; then # TODO: get those shorthands from the components.py script directly
 run_type="${NEW_RUN_MODE?}"
else
 run_type="${RE_RUN_MODE?}"
fi
echo "run_type=\"${run_type?}\""

# Creates attempt dir structure
source helpers/init.sh ${etl_dir?} ${release_name?} ${patch_number?} ${run_type?}

release_cluster_output_dir="${overarch_cluster_output_dir?}/${release_name?}"
patch_cluster_output_dir="${release_cluster_output_dir?}/${patch_number?}"
run_cluster_output_dir="${patch_cluster_output_dir?}/${run_number?}"
run_fuse_output_dir="${fuse_mount_point?}${run_cluster_output_dir?}"

validator_jar_file_name="dcc-validator.jar"
validator_jar_dir=$(dirname ${etl_jar_file?}) # They are expected to live in the same directory for the now
validator_jar_file="${validator_jar_dir?}/${validator_jar_file_name?}"

[ -f "${config_file?}" ] || { echo "ERROR: cannot find config: \"${config_file?}\""; exit 1; }

nn_server=$(get_config_info ${etl_jar_file?} ${config_file?} "NAMENODE" "HOST")
jt_server=$(get_config_info ${etl_jar_file?} ${config_file?} "JOBTRACKER" "HOST")
es_server=$(get_config_info ${etl_jar_file?} ${config_file?} "ELASTIC_SEARCH" "HOST")
mongo_server=$(get_config_info ${etl_jar_file?} ${config_file?} "MONGO_ETL_NORMAL" "HOST")
[ -n "$(grep ${es_server?} ${config_file?})" ] || { echo "ERROR: couldn't find ${es_server?} in config file..."; exit 1; }
[ -n "$(grep ${nn_server?} ${config_file?})" ] || { echo "ERROR: couldn't find ${nn_server?} in config file..."; exit 1; }
[ -n "$(grep ${jt_server?} ${config_file?})" ] || { echo "ERROR: couldn't find ${jt_server?} in config file..."; exit 1; }
[ -n "$(grep ${mongo_server?} ${config_file?})" ] || { echo "ERROR: couldn't find ${mongo_server?} in config file..."; exit 1; }
ping -c1 "${mongo_server?}" && echo $?
ping -c1 "${es_server?}" && echo $?
ping -c1 "${nn_server?}" && echo $?
ping -c1 "${jt_server?}" && echo $?
echo

[ -f "${projects_json_file?}" ] || { echo "ERROR: ${projects_json_file?} does not exist"; exit 1; }
project_keys=$(python_utils "get_project_keys(\"${projects_json_file?}\")")
project_count=$(echo "${project_keys?}" | tr ' ' '\n' | wc -l | awk '{print $1}')

valid_components="${concatenator_component?},${pruner_component?},${normalizer_component?},${annotator_component?},${loader_component?},${importer_component?},${summarizer_component?},${indexer_component?},${stats_component?},${exporter_component?}"
components_to_run_tmp=$(helpers/components.py "${valid_components?}" ${components_to_run?} | tail -n1)
[ -n "$(echo ${components_to_run_tmp?} | awk '/^ok: /')" ] || { echo "ERROR: invalid components to run: ${components_to_run?}"; exit 1; }
components_to_run=$(echo ${components_to_run_tmp?} | sed 's/ok: //')
current_component_cluster_output_dir="N/A"

export job_id="${release_name?}-${patch_number?}-${run_number?}"

# ===========================================================================
# Init attempt:
attempt_manifest_file="${attempt_output_dir?}/MANIFEST"
echo "job dir: ${attempt_output_dir?}" >> ${attempt_manifest_file?}
echo >> ${attempt_manifest_file?}
echo "user: $SUDO_USER" >> ${attempt_manifest_file?}
echo "timestamp: $(timestamp) ($(date '+%s'))" >> ${attempt_manifest_file?}
echo "ETL dir: ${etl_dir?}" >> ${attempt_manifest_file?}
echo "Components: ${components_to_run?}" >> ${attempt_manifest_file?}
echo "job ID: ${job_id?}" >> ${attempt_manifest_file?}
echo "release name: ${release_name?}" >> ${attempt_manifest_file?}
echo "patch number: ${patch_number?}" >> ${attempt_manifest_file?}
echo "run number: ${run_number?}" >> ${attempt_manifest_file?}
echo "attempt number: ${attempt_number?}" >> ${attempt_manifest_file?}
echo "project count: ${project_count?}" >> ${attempt_manifest_file?}
echo "project keys: ${project_keys?}" >> ${attempt_manifest_file?}
echo "projects file: ${projects_json_file?}" >> ${attempt_manifest_file?}
echo "dictionary file: ${dictionary_file?}" >> ${attempt_manifest_file?}
echo "codelists file: ${codelists_file?}" >> ${attempt_manifest_file?}
echo "default parent data dir: ${default_parent_data_dir?}" >> ${attempt_manifest_file?}
#echo "Validator commit ID: $(get_commit_id ${validator_jar_file?})" >> ${attempt_manifest_file?}
echo "ETL commit ID: $(get_commit_id ${etl_jar_file?})" >> ${attempt_manifest_file?}

attempt_tree_dir="${attempt_output_dir?}/trees"
mkdir ${attempt_tree_dir?}

attempt_default_tree_file="${attempt_tree_dir?}/default"
tree "${fuse_mount_point?}${default_parent_data_dir?}" >> ${attempt_default_tree_file?}

attempt_etl_dir_tree_file="${attempt_tree_dir?}/etl_dir"
tree "${etl_dir?}" >> ${attempt_etl_dir_tree_file?}

cp -r ${PWD?} ${attempt_output_dir?}/scripts
cp ${projects_json_file?} ${attempt_output_dir?}/projects.json
cp ${etl_jar_file?} ${attempt_output_dir?}/dcc-etl.jar
cp ${validator_jar_file?} ${attempt_output_dir?}/dcc-validator.jar
cp ${config_file?} ${attempt_output_dir?}/config.yaml
cp ${dictionary_file?} ${attempt_output_dir?}/dictionary.json

export attempt_output_dir && export attempt=${attempt_output_dir?}

# ===========================================================================

echo "etl_dir=\"${etl_dir?}\""
echo "job_id=\"${job_id?}\""
echo "attempt_number=\"${attempt_number?}\""
echo "default_parent_data_dir=\"${default_parent_data_dir?}\""
echo "projects_json_file=\"${projects_json_file?}\""
echo "dictionary_file=\"${dictionary_file?}\""
echo "project_keys=\"${project_keys?}\""
echo "config_file=\"${config_file?}\""
echo "mongo_server=\"${mongo_server?}\""
echo "fuse_mount_point=\"${fuse_mount_point?}\""
echo "components_to_run=\"${components_to_run?}\""
echo "run_type=\"${run_type?}\""
echo "es_server=\"${es_server?}\""
echo "nn_server=\"${nn_server?}\""
echo "mongo_server=\"${mongo_server?}\""

# ===========================================================================

email ${config_file?} "started"

# ===========================================================================
# TODO: parallelize components AND projects
# ===========================================================================
# Concatenator

component="${concatenator_component?}" && register_new_component ${component?}

new_cmd_builder
add_to_cmd "facades/${component?}-facade.sh"
add_to_cmd "  ${etl_dir?}"
add_to_cmd "  ${default_parent_data_dir?}"
add_to_cmd "  ${projects_json_file?}"
add_to_cmd "  ${etl_jar_file?}"
add_to_cmd "  ${dictionary_file?}"
add_to_cmd "  ${config_file?}"
add_to_cmd "  ${run_cluster_output_dir?}"
eval_cmd_if_component_to_run "$(build_cmd)" "${components_to_run?}" "${component?}" "${component_log_file?}"
echo

# ===========================================================================
# Pruner

component="pruner" && register_new_component ${component?}

new_cmd_builder
add_to_cmd "facades/${component?}-facade.sh"
add_to_cmd "  ${projects_json_file?}"
add_to_cmd "  ${etl_jar_file?}"
add_to_cmd "  ${dictionary_file?}"
add_to_cmd "  ${config_file?}"
add_to_cmd "  ${run_cluster_output_dir?}"
eval_cmd_if_component_to_run "$(build_cmd)" "${components_to_run?}" "${component?}" "${component_log_file?}"
echo

# ===========================================================================
# Normalizer

component="normalizer" && register_new_component ${component?}

new_cmd_builder
add_to_cmd "facades/${component?}-facade.sh"
add_to_cmd "  \"${project_keys?}\""
add_to_cmd "  ${validator_jar_file?}"
add_to_cmd "  hdfs://${nn_server?}" # TODO: get from config
add_to_cmd "  ${jt_server?}:8021" # TODO: get from config
add_to_cmd "  ${run_cluster_output_dir?}"
eval_cmd_if_component_to_run "$(build_cmd)" "${components_to_run?}" "${component?}" "${component_log_file?}"
echo

# ===========================================================================
# Annotator

component="annotator" && register_new_component ${component?}
fs_url=$(get_config_value ${config_file} fsUrl)
mapred_job_tracker=${jt_server?}:8021 # TODO: get from config

new_cmd_builder
add_to_cmd "facades/${component?}-facade.sh"
add_to_cmd "  ${etl_dir?}"
add_to_cmd "  ${fs_url?}"
add_to_cmd "  ${mapred_job_tracker?}"
add_to_cmd "  ${run_cluster_output_dir?}"
add_to_cmd "  \"${project_keys?}\""
eval_cmd_if_component_to_run "$(build_cmd)" "${components_to_run?}" "${component?}" "${component_log_file?}"
echo

# ===========================================================================
# Loader
component="loader" && register_new_component ${component?}

new_cmd_builder
add_to_cmd "facades/${special_component?}-facade.sh"
add_to_cmd "  ${etl_jar_file?}"
add_to_cmd "  ${dictionary_file?}"
add_to_cmd "  ${codelists_file?}"
add_to_cmd "  ${run_cluster_output_dir?}"
add_to_cmd "  ${job_id?}"
add_to_cmd "  ${release_prefix?}"
add_to_cmd "  ${release_number?}"
add_to_cmd "  ${patch_number?}"
add_to_cmd "  ${run_number?}"
add_to_cmd "  \"${project_keys?}\""
add_to_cmd "  ${config_file?}"
add_to_cmd "  load"
add_to_cmd "  ${index_type?}"
eval_cmd_if_component_to_run "$(build_cmd)" "${components_to_run?}" "${component?}" "${component_log_file?}"
echo

# ===========================================================================
# Importer
component="importer" && register_new_component ${component?}

new_cmd_builder
add_to_cmd "facades/${special_component?}-facade.sh"
add_to_cmd "  ${etl_jar_file?}"
add_to_cmd "  ${dictionary_file?}"
add_to_cmd "  ${codelists_file?}"
add_to_cmd "  ${run_cluster_output_dir?}"
add_to_cmd "  ${job_id?}"
add_to_cmd "  ${release_prefix?}"
add_to_cmd "  ${release_number?}"
add_to_cmd "  ${patch_number?}"
add_to_cmd "  ${run_number?}"
add_to_cmd "  \"${project_keys?}\""
add_to_cmd "  ${config_file?}"
add_to_cmd "  import"
add_to_cmd "  ${index_type?}"
eval_cmd_if_component_to_run "$(build_cmd)" "${components_to_run?}" "${component?}" "${component_log_file?}"
echo

# ===========================================================================
# Summarizer
component="summarizer" && register_new_component ${component?}

new_cmd_builder
add_to_cmd "facades/${special_component?}-facade.sh"
add_to_cmd "  ${etl_jar_file?}"
add_to_cmd "  ${dictionary_file?}"
add_to_cmd "  ${codelists_file?}"
add_to_cmd "  ${run_cluster_output_dir?}"
add_to_cmd "  ${job_id?}"
add_to_cmd "  ${release_prefix?}"
add_to_cmd "  ${release_number?}"
add_to_cmd "  ${patch_number?}"
add_to_cmd "  ${run_number?}"
add_to_cmd "  \"${project_keys?}\""
add_to_cmd "  ${config_file?}"
add_to_cmd "  summarize"
add_to_cmd "  ${index_type?}"
eval_cmd_if_component_to_run "$(build_cmd)" "${components_to_run?}" "${component?}" "${component_log_file?}"
echo

# ===========================================================================
# Indexer
component="indexer" && register_new_component ${component?}

new_cmd_builder
add_to_cmd "facades/${special_component?}-facade.sh"
add_to_cmd "  ${etl_jar_file?}"
add_to_cmd "  ${dictionary_file?}"
add_to_cmd "  ${codelists_file?}"
add_to_cmd "  ${run_cluster_output_dir?}"
add_to_cmd "  ${job_id?}"
add_to_cmd "  ${release_prefix?}"
add_to_cmd "  ${release_number?}"
add_to_cmd "  ${patch_number?}"
add_to_cmd "  ${run_number?}"
add_to_cmd "  \"${project_keys?}\""
add_to_cmd "  ${config_file?}"
add_to_cmd "  index"
add_to_cmd "  ${index_type?}"
eval_cmd_if_component_to_run "$(build_cmd)" "${components_to_run?}" "${component?}" "${component_log_file?}"
echo

# ===========================================================================
# Stats

component="stats" && register_new_component ${component?}

new_cmd_builder
add_to_cmd "facades/${special_component?}-facade.sh"
add_to_cmd "  ${etl_jar_file?}"
add_to_cmd "  ${dictionary_file?}"
add_to_cmd "  ${codelists_file?}"
add_to_cmd "  ${run_cluster_output_dir?}"
add_to_cmd "  ${job_id?}"
add_to_cmd "  ${release_prefix?}"
add_to_cmd "  ${release_number?}"
add_to_cmd "  ${patch_number?}"
add_to_cmd "  ${run_number?}"
add_to_cmd "  \"${project_keys?}\""
add_to_cmd "  ${config_file?}"
add_to_cmd "  stats"
add_to_cmd "  ${index_type?}"
#eval_cmd_if_component_to_run "$(build_cmd)" "${components_to_run?}" "${component?}" "${component_log_file?}"
echo

# ===========================================================================
# Exporter
component="exporter" && register_new_component ${component?}

new_cmd_builder
add_to_cmd "facades/${component?}-facade.sh"
add_to_cmd "  ${job_id?}"
add_to_cmd "  ${run_cluster_output_dir?}"
add_to_cmd "  ${etl_dir?}"
eval_cmd_if_component_to_run "$(build_cmd)" "${components_to_run?}" "${component?}" "${component_log_file?}"

echo
# ===========================================================================
# Vcf:

original_vcf_file="${run_cluster_output_dir?}/${indexer_component?}/*.vcf.*"
if [ -f ${fuse_mount_point?}/${original_vcf_file?} ]; then
	summary_dir="/tmp/download/static/${job_id?}/Summary"
	command="hadoop fs -mkdir -p ${summary_dir?}" && echo ${command?} && eval ${command?} || : # Should already exist but just in case
	command="hadoop fs -cp ${original_vcf_file?} ${summary_dir?}/" && echo ${command?} && eval ${command?}
	command="hadoop fs -put ./VCF_READ_ME ${summary_dir?}/README.txt" && echo ${command?} && eval ${command?}
else
	echo "Couldn't find '${fuse_mount_point?}/${original_vcf_file?}'"
fi

# ===========================================================================

echo && echo "SUCCESS" && echo
email ${config_file?} "finished"
timestamp
exit

