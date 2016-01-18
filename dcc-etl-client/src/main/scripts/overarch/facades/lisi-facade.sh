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

# Facade script around the loader/importer/summarizer/indexer/stats components
# usage: See call in parent script
# point person: Anthony and Bob (DCC-959)

# ===========================================================================
# Include dependency
source helpers/utils.sh
source helpers/cmd_builder.sh
source helpers/constants.sh

# ---------------------------------------------------------------------------
# Sanity checks
ensure_user "dcc_dev"
ensure_pwd "overarch"

# ---------------------------------------------------------------------------

jar_file=${1?} && shift
dictionary_file=${1?} && shift
codelists_file=${1?} && shift
parent_output_dir=${1?} && shift
job_id=${1?} && shift
release_prefix=${1?} && shift
release_number=${1?} && shift
patch_number=${1?} && shift
run_number=${1?} && shift
project_keys=${1?} && shift # whitespace-separated
config_file=${1?} && shift
action=${1?} && shift # see further down for all options
index_type=$1 # optional

logback_file="$HOME/dcc-etl/conf/logback.xml" # in git

# ---------------------------------------------------------------------------
# actions (verbs based on component names):

# Check input provided
all_actions="all"
importer_action="import"
loader_action="load"
summarizer_action="summarize"
indexer_action="index"
stats_action="stats"

valid_actions="${all_actions?} ${loader_action?} ${importer_action?} ${indexer_action?} ${summarizer_action?} ${stats_action?}" # only support these for now
[ -n "$(echo "${valid_actions?}" | tr " " "\n" | awk '$0=="'"${action?}"'"')" ] || { echo "ERROR: invalid action: \"${action?}\""; exit 1; }

# Build the action argument option for the java call
action_arg=""
if [ "${action?}" != "${all_actions?}" ]; then
 action_arg="${action?}"
fi

# ---------------------------------------------------------------------------
# TODO: remove after point (2) of https://wiki.oicr.on.ca/display/DCCSOFT/Data+fixing#Datafixing-Indexer is done
if [ "${index_type?}" == "" ]; then
 index_type_option="" # not specified
else
 index_type_option=" --index-types ${index_type?}"
fi
index_alias="current"

# ===========================================================================
# Check etl isn't already running
jar_name=$(basename "${jar_file?}")
[ -z "$(jps | grep ${jar_name?})" ] || { echo "ERROR: must kill all previously started etl run: \"$(jps | grep ${jar_name?})\""; exit 1; }

# ===========================================================================

if [ "${action_arg?}" == "${loader_action?}" -o "${action_arg?}" == "${all_actions?}" ]; then
	admin_database_name="admin" # constant in mongodb
	mongo_server=$(get_config_info ${jar_file?} ${config_file?} "MONGO_ETL_NORMAL" "HOST")
	 admin_database_user=$(get_config_info ${jar_file?} ${config_file?} "MONGO_ETL_ADMIN" "USERNAME") # formerly "admin" on dev
	normal_database_user=$(get_config_info ${jar_file?} ${config_file?} "MONGO_ETL_NORMAL" "USERNAME")
	 admin_database_user_passwd=$(get_config_info ${jar_file?} ${config_file?} "MONGO_ETL_ADMIN" "PASSWORD")
	normal_database_user_passwd=$(get_config_info ${jar_file?} ${config_file?} "MONGO_ETL_NORMAL" "PASSWORD")

	# ---------------------------------------------------------------------------
	new_cmd_builder
	add_to_cmd "helpers/mongo.sh"
	add_to_cmd "  ${mongo_server?}"
	add_to_cmd "  ${admin_database_name?}"
	add_to_cmd "  ${admin_database_user?}"
	add_to_cmd "  ${admin_database_user_passwd?}"
	add_to_cmd "  ${job_id?}" # = database name
	add_to_cmd "  ${normal_database_user?}"
	add_to_cmd "  ${normal_database_user_passwd?}"
	cmd=$(build_cmd)
	pretty_print_cmd "${cmd?}"
	eval_cmd "${cmd?}"
fi

# ---------------------------------------------------------------------------
printf '.%.0s' {1..75} && echo

new_cmd_builder
add_to_cmd "java"
add_to_cmd "  -Xmx12g -Xms12g" # mostly for the gene-donor summarizer for now
add_to_cmd "  -Dlogback.configurationFile=${logback_file?}"
add_to_cmd "  -Djava.library.path=/usr/lib/hadoop/lib/native"
add_to_cmd " -jar ${jar_file?}"

add_to_cmd " --config ${config_file?}"

add_to_cmd " --dictionary ${dictionary_file?}"
add_to_cmd " --codelists ${codelists_file?}"

add_to_cmd " --working-dir ${parent_output_dir?}" # contains working dirs for each components (inputs in: concatenator, normalizer and annotator, outputs in: loader, indexer)

add_to_cmd " --job-id ${job_id?}"

add_to_cmd " --release-prefix ${release_prefix?}"
add_to_cmd " --release-number ${release_number?}"
add_to_cmd " --patch-number ${patch_number?}"
add_to_cmd " --run-number ${run_number?}"

add_to_cmd " --projects $(echo ${project_keys?} | tr ' ' ',')"

add_to_cmd " --alias ${index_alias?}"
add_to_cmd " ${index_type_option?}"
add_to_cmd " ${action_arg?}"
cmd=$(build_cmd)
pretty_print_cmd "${cmd?}"
eval_cmd "${cmd?}"

# ===========================================================================

