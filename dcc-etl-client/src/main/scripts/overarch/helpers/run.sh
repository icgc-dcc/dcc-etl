#!/bin/bash
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

# Temporary script to help run the overarching script in different clusters (until it stabilizes)
# usage: helpers/run.sh myrelease myjar myprojects > mylogs
set -o nounset

# ===========================================================================

git_dir=${1?} && shift # ***REMOVED***/dcc-etl/git/dcc-etl
jar_file_name=${1?} && shift
projects_file_name=${1?} && shift
release_prefix=${1?} && shift
components_to_run=${1?} && shift

# ===========================================================================

host=$(hostname -f)
valid_host=false

if [ "$host" == "***REMOVED***" ]; then
 cluster="prod"
 use_docker=false
 valid_host=true
fi

if [ "$host" == "hcn-135.res.oicr.on.ca" ]; then
 cluster="prod"
 use_docker=true
 valid_host=true
fi

if [ "$host" == "***REMOVED***" ]; then
 cluster="prod"
 use_docker=false
 valid_host=true
fi

if [ "$host" == "***REMOVED***" ]; then
 cluster="dev"
 use_docker=false
 valid_host=true
fi

if [ "$host" == "dcc-etl-main" ]; then
 cluster="dev"
 use_docker=false
 valid_host=true
fi

if ! $valid_host; then
 echo "ERROR: invalid host: ${host?}"
 exit 1
fi

# ===========================================================================

overarch_dir=dcc-etl-client/src/main/scripts/overarch
real_etl_dir=***REMOVED***/dcc-etl

if $use_docker; then
 etl_dir="/etl"
else
 etl_dir=${real_etl_dir?}
fi

real_jar_file=${real_etl_dir?}/lib/${jar_file_name?}
[ -f ${real_jar_file?} ] || read -p "ERROR: jar must exist ('${real_jar_file?}')"
if $use_docker; then
 ! [ -h ${real_jar_file?} ] || { # links don't work with docker
	jar_file_name="dcc-etl-docker.jar"
	cp ${real_jar_file?} ${real_etl_dir?}/lib/${jar_file_name?}
  }
fi

release_number=20
patch_number=0
dictionary_version="0.12e"
release_name="${release_prefix?}${release_number?}"
default_parent_dir="/icgc/submission/ICGC${release_number?}"
overarch_command="${overarch_dir?}/overarch.sh ${etl_dir?} ${release_prefix?} ${release_number?} ${patch_number?} ${default_parent_dir?} ${etl_dir?}/conf/projects/${projects_file_name?} ${etl_dir?}/lib/${jar_file_name?} ${etl_dir?}/conf/etl_${cluster?}.yaml ${etl_dir?}/conf/dictionaries/${dictionary_version?}.json ${etl_dir?}/conf/codelists.json ${components_to_run?}"
echo $overarch_command

# ===========================================================================

cd ${git_dir?}
pwd
if $use_docker; then
 docker_command="dcc-etl-client/src/main/scripts/overarch/helpers/cdh-docker.sh \"${git_dir?}\" \"${overarch_command?}\""
 echo ${docker_command?}
 eval ${docker_command?}
else
 eval "${overarch_command?}"
fi

# ===========================================================================
