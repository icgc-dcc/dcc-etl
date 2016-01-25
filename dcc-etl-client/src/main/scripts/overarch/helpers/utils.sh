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

# TODO: split utils and dcc-utils

function ensure_user() {
	expected=${1?}
	[ "${USER?}" == "${expected?}" ] || { echo "ERROR: must run script as \"${expected?}\" user, not \"${USER?}\""; exit 1; }
}

function ensure_pwd() {
	expected=${1?}
	[ "$(basename ${PWD?})" == "${expected?}" ] || { echo "ERROR: must run script from \"${expected?}\" directory, not \"${PWD?}\""; exit 1; }
}

function get_dictionary_version() {
	dictionary_file=${1?}
	cat ${dictionary_file?} | python -c "import json,sys;print json.loads(sys.stdin.read())[\"version\"];"
}

#@Deprecated
function download_dictionary() {
	dictionary_output_file=${1?}

	parent_output_dir=$(dirname ${dictionary_output_file?})
	echo "parent_output_dir=\"${parent_output_dir?}\""

	create_dir_if_doesnt_exist ${parent_output_dir?}

	echo "DEPRECATED... This points to the public facing app"
	curl -s -H "Accept: application/json" <dictionary_host> > ${dictionary_output_file?}


	dictionary_version=$(get_dictionary_version ${dictionary_output_file?})
	[ -n "${dictionary_version?}" ] || { echo "ERROR: Could not find a version in dictionary file: '${dictionary_output_file?}'"; exit 1; } # Sanity check
}

function send_overarch_email() {
	run_name=${1?} && shift
	overarch_parent_output_dir=${1?}
	subject="Overarch: ETL run '${run_name?}' has finished"
	body="$(tree --charset iso-8859 ${overarch_parent_output_dir?})"
	recipients="<comma_separated_emails>" # TODO: Set valid email
	echo "Sending email '${subject?}' to '${recipients?}':\\n${body?}"
	echo -e "${body?}" | mail -s "${subject?}" "${recipients?}"
}

# Prints a line of dots to help separate stdout
function print_stdout_section_separator() {
	echo
	printf '.%.0s' {1..75}
	echo
}

# Creates a dir (recursively) if it doesn't exist
function create_dir_if_doesnt_exist() {
	dir=${1?}
	if [ ! -d "${dir?}" ]; then
	  echo "Creating dir: '${dir?}'"
	  mkdir -p ${dir?}
	else
	  echo "Dir '${dir?}' already exists"
	fi
}

function get_config_info() {
 jar_file=${1?} && shift
 config_file=${1?} && shift
 type=${1?} && shift
 sub_type=${1?} && shift

 java \
   -cp ${jar_file?} \
   org.icgc.dcc.etl.core.config.Main \
   ${config_file?} \
   ${type?} \
   ${sub_type?}
}

function get_config_value() { # TODO: use yaml parser
	config_file=${1?} && shift
	config_key=${1?} && shift
	awk '/^'"${config_key?}"' *:.+$/' ${config_file?} | sed -r 's/.+"([^"]+)".*/\1/'
}

function only_integers() {
 awk '/^[0-9]+$/'
}

function list_content() {
 dir=${1?} && shift
 ls ${dir?} | only_integers | sort -k1n,1n | tr '\n' ',' | sed 's/,$//'
}

function get_latest_number() {
    dir=${1?} && shift
    if [ -d "${dir?}" ]; then
        count=$(ls -1 "${dir?}" | only_integers | wc -l | awk '{print $1}')
		if [ "${count?}" == "0" ]; then
            echo -1
        else
            echo $(ls -1 ${dir?} | only_integers | sort -k1n,1n | tail -n1)
        fi
    else
        echo -1
    fi
}

function get_commit_id() {
	jar_file=${1?}
	java -jar ${jar_file?} --version | awk '/ +git\.commit\.id *:/' | sed -r 's/.+: *(.+)$/\1/'
}

# Runs a string-returning function from helpers/utils.py; usage: my_value=$(python_utils "my_utils_function('my param')")
function python_utils() {
  call=${1?}
  python -c "import sys; sys.path.append('helpers'); import utils; sys.stdout.write(utils.${call?})"
}

function email() {
	config_file=${1?} && shift
	status=${1?} && shift

	smtp_server=$(get_config_value ${config_file?} ${SMTP_SERVER_CONFIG_KEY?})
	sender=$(get_config_value ${config_file?} ${SENDER_CONFIG_KEY?})
	recipients=$(get_config_value ${config_file?} ${RECIPIENTS_CONFIG_KEY?})
  subject="ETL run '${job_id?}' on '${HOSTNAME?}' ${status?}"
  body="$(cat ${attempt_manifest_file?})"
  echo -e "${body?}" | mail -s "${subject?}" "${recipients?}" -aFrom:${sender?}
}

# Determines whether the component is to be run or not
function is_component_to_run() {
  components_to_run=${1?} && shift
  component=${1?}
  [ -n "$(echo "${components_to_run?}" | tr " " "\n" | awk '$0=="'"${component?}"'"')" ]
}

# ===========================================================================

