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


# TODO: use pig's DIFF rather

export HADOOP_USER_NAME=hdfs
date "+%y%m%d%H%M%S"
start=$(date +%s)

# ===========================================================================

etl_run_name=load-prod-06e-41
etl_run_number=${1?} && shift
test_run_number=${1?} && shift
types_csv=${1?} && shift # "ssm,cnsm,stsm,mirna,meth,exp,pexp,jcn"
projects_csv=$1

if [ "$HOSTNAME" == "hproxy-dev" ]; then
 fake_input_dir="/icgc/testing/prod/normalizer/${etl_run_name?}-${etl_run_number?}"
 static_input_dir="/icgc/testing/prod/download/static/${etl_run_name?}-${etl_run_number?}"
 default_parallel=12
 child_memory=2 # in GB
fi
if [ "$HOSTNAME" == "hproxy-dcc" ]; then
 fake_input_dir="/icgc/normalizer/${etl_run_name?}-${etl_run_number?}"
 static_input_dir="/icgc/download/static/release_14"
 default_parallel=45
 child_memory=6 # in GB
fi

# ===========================================================================

for type in $(echo ${types_csv?} | tr ',' ' '); do
 printf '=%.0s' {1..75} && echo && echo "diff: ${type?}"
 type_start=$(date +%s)

 if [ -z "$projects_csv" ]; then
  project_ids=$(find /hdfs/dcc${fake_input_dir?} | awk '/\/'${type?}'/' | awk -F$'/' '{print $(NF-1)}' | sort -u | tr '\n' ' ')
  if false; then
   echo "Sourcing subset of projects"
   source ./subset.sh
  fi
 else
  project_ids=$(echo ${projects_csv?} | tr ',' ' ')
 fi
 echo "project_ids=\"${project_ids?}\""

 # ---------------------------------------------------------------------------

 fake_command="./fake.sh ${etl_run_name?} ${etl_run_number?} ${test_run_number?} ${type?} ${fake_input_dir?} $(echo ${project_ids?} | tr ' ' ',') ${default_parallel?} ${child_memory?}" # ./fake.sh load-prod-06e-41 27 53 stsm /icgc/testing/prod/normalizer/load-prod-06e-41-27
 echo "${fake_command?}"
 eval "${fake_command?}"

 # ---------------------------------------------------------------------------

 static_command="./static.sh ${etl_run_name?} ${etl_run_number?} ${test_run_number?} ${type?} ${fake_input_dir?} ${static_input_dir?} $(echo ${project_ids?} | tr ' ' ',') ${default_parallel?} ${child_memory?}" # ./static.sh load-prod-06e-41 27 53 stsm /icgc/testing/prod/normalizer/load-prod-06e-41-27 /icgc/testing/prod/download/static/load-prod-06e-41-27
 echo "${static_command?}"
 eval "${static_command?}"

 # ---------------------------------------------------------------------------

 echo "project_ids=\"${project_ids?}\""

 for project_id in ${project_ids?}; do
  echo -n ${project_id?}
  fake_output_dir="/hdfs/dcc/icgc/testing/fake/${etl_run_name?}-${etl_run_number?}-${test_run_number?}/${project_id?}.${type?}"
  static_output_dir="/hdfs/dcc/icgc/testing/static/${etl_run_name?}-${etl_run_number?}-${test_run_number?}/${project_id?}.${type?}"
  [ -z "$(diff -q <(zcat ${fake_output_dir?}/part-*) <(zcat ${static_output_dir?}/part-*))" ] && echo ": OK - ${fake_output_dir?} == ${static_output_dir?}" || echo ": KO... - ${fake_output_dir?} != ${static_output_dir?}"
 done

 type_end=$(date +%s) && echo "diff: ${type?}: all: ellapsed time: $[type_end-type_start]"
done

# ===========================================================================

echo done.

end=$(date +%s) && echo "diff: all: all: ellapsed time: $[end-start]"
date "+%y%m%d%H%M%S"

