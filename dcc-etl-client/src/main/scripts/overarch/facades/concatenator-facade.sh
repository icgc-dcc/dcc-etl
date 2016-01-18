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

# Facade script around the concatenator
# usage: See call in parent script
# point person: Anthony

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
default_parent_data_dir=${1?} && shift
projects_json_file=${1?} && shift
jar_file=${1?} && shift
dictionary_file=${1?} && shift
config_file=${1?} && shift
parent_cluster_output_dir=${1?} && shift

echo "default_parent_data_dir=\"${default_parent_data_dir?}\""
echo "projects_json_file=\"${projects_json_file?}\""
echo "jar_file=\"${jar_file?}\""
echo "dictionary_file=\"${dictionary_file?}\""
echo "config_file=\"${config_file?}\""
echo "parent_cluster_output_dir=\"${parent_cluster_output_dir?}\""

# ===========================================================================
# constants:

main_class="org.icgc.dcc.etl.concatenator.Main"
logback_file="$HOME/dcc-etl/conf/logback.xml" # in git

# ===========================================================================

echo
print_stdout_section_separator
new_cmd_builder
add_to_cmd "java"
add_to_cmd "   -Dlogback.configurationFile=${logback_file?}"
add_to_cmd "  -cp ${jar_file?}"
add_to_cmd "  ${main_class?}"
add_to_cmd "  -D ${default_parent_data_dir?}"
add_to_cmd "  -p ${projects_json_file?}"
add_to_cmd "  -d ${dictionary_file?}"
add_to_cmd "  -c ${config_file?}"
add_to_cmd "  -o ${parent_cluster_output_dir?}/${concatenator_component?}"
cmd=$(build_cmd)
pretty_print_cmd "${cmd?}"
eval_cmd "${cmd?}"

# ===========================================================================

