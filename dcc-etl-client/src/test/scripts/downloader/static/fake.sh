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

# TODO: use backticks in param file

export HADOOP_USER_NAME=hdfs
date "+%y%m%d%H%M%S"
start=$(date +%s)

# ===========================================================================

etl_run_name=${1?} && shift
etl_run_number=${1?} && shift
test_run_number=${1?} && shift
type=${1?} && shift
fake_input_dir=${1?} && shift
project_ids=${1?} && shift # as CSV
default_parallel=${1?} && shift
child_memory=${1?} && shift
dry_run=$1 # anything non-empty

# ===========================================================================

function get_clinical_file() { ls /hdfs/dcc${fake_input_dir?}/${3?}/*${4?}* | sed 's/\/hdfs\/dcc//'; }
function get_experimental_file() { ls /hdfs/dcc${fake_input_dir?}/${3?}/${4?}*__${5?}* | sed 's/\/hdfs\/dcc//'; }
function get_schema() { head -n1 /hdfs/dcc/${1?} | tr '\t' '\n' | awk '{print $0 ": chararray"}' | tr '\n' ',' | sed 's/,$//' | awk '{gsub(/,/,", ")}1'; }
function get_normal() { head -n1 /hdfs/dcc/${1?} | tr '\t' '\n' | awk '{print "TRIM(CLEAR((chararray)" $0 ")) AS " $0}' | tr '\n' ',' | sed 's/,$//' | awk '{gsub(/,/,", ")}1'; }
function has_secondary() { [ -n "$(echo 'ssm cnsm stsm mirna meth' | tr ' ' '\n' | awk '$0=="'"${1?}"'"')" ]; }

# ===========================================================================

for project_id in $(echo ${project_ids?} | tr ',' ' '); do
 printf '=%.0s' {1..75} && echo && echo "fake: ${project_id?}"
 project_start=$(date +%s)

 donor_file=$(get_clinical_file ${etl_run_name?} ${etl_run_number?} ${project_id?} donor)
 specimen_file=$(get_clinical_file ${etl_run_name?} ${etl_run_number?} ${project_id?} specimen)
 sample_file=$(get_clinical_file ${etl_run_name?} ${etl_run_number?} ${project_id?} sample)

 m_file=$(get_experimental_file ${etl_run_name?} ${etl_run_number?} ${project_id?} ${type?} m)

 if [ "${type?}" == "exp" ]; then
  p_file=$(get_experimental_file ${etl_run_name?} ${etl_run_number?} ${project_id?} ${type?} g) # exp is an exception (uses exp_g as primary file)
 else
  if [ "${type?}" == "cnsm" -a "${project_id?}" == "LAML-KR" ]; then # the "__pol" part of the name is problematic otherwise
   p_file="${fake_input_dir?}/LAML-KR/cnsm__kr__15__pol__p__60__20121115.txt"
  else
   p_file=$(get_experimental_file ${etl_run_name?} ${etl_run_number?} ${project_id?} ${type?} p)
  fi
 fi

 if $(has_secondary ${type?}); then
  if [ "${type?}" == "meth" -a "${project_id?}" != "CLLE-ES" ]; then # CLLE-ES is the only project with meth_s
   s_file="/icgc/meta/headers/0.6e/meth__s.txt"
  else
   s_file=$(get_experimental_file ${etl_run_name?} ${etl_run_number?} ${project_id?} ${type?} s)
  fi
 fi # Some types don't have secondary data

 output_dir="/icgc/testing/fake/${etl_run_name?}-${etl_run_number?}-${test_run_number?}/${project_id?}.${type?}"

 param_file="/nfs/dcc_secure/dcc/metadata/testing/downloader/fake/${project_id?}.${type?}.param"
 echo > ${param_file?}

 echo "etl_run_name=\"${etl_run_name?}\"" >> ${param_file?}
 echo "etl_run_number=\"${etl_run_number?}\"" >> ${param_file?}
 echo "test_run_number=\"${test_run_number?}\"" >> ${param_file?}
 echo "project_id=\"${project_id?}\"" >> ${param_file?}
 echo >> ${param_file?}

 echo "donor_file=\"${donor_file?}\"" >> ${param_file?}
 echo "specimen_file=\"${specimen_file?}\"" >> ${param_file?}
 echo "sample_file=\"${sample_file?}\"" >> ${param_file?}
 echo >> ${param_file?}

 echo "m_file=\"${m_file?}\"" >> ${param_file?}
 echo "p_file=\"${p_file?}\"" >> ${param_file?}
 if $(has_secondary ${type?}); then echo "s_file=\"${s_file?}\"" >> ${param_file?}; fi
 echo >> ${param_file?}

 echo "m_fields=\"$(get_schema ${m_file?})\"" >> ${param_file?}
 echo "p_fields=\"$(get_schema ${p_file?})\"" >> ${param_file?}
 if $(has_secondary ${type?}); then echo "s_fields=\"$(get_schema ${s_file?})\"" >> ${param_file?}; fi
 echo >> ${param_file?}

 echo "m_normal=\"$(get_normal ${m_file?})\"" >> ${param_file?}
 echo "p_normal=\"$(get_normal ${p_file?})\"" >> ${param_file?}
 if $(has_secondary ${type?}); then echo "s_normal=\"$(get_normal ${s_file?})\"" >> ${param_file?}; fi
 echo >> ${param_file?}

 echo "output_dir=\"${output_dir?}\"" >> ${param_file?}
 echo >> ${param_file?}

 echo "default_parallel=\"${default_parallel?}\"" >> ${param_file?}
 echo >> ${param_file?}

 echo "cat ${param_file?}"
 cat ${param_file?}

 dry_run_flag=$([ -n "$dry_run" ] && echo "-r " || echo "")
 pig_command=""
 pig_command="${pig_command?} pig"
 pig_command="${pig_command?}    -Dmapred.child.java.opts=\"-Xmx${child_memory?}g\""
 pig_command="${pig_command?}    -Dmapred.compress.map.output=true"
 pig_command="${pig_command?}    -Dmapred.output.compression.type=\"BLOCK\""
 pig_command="${pig_command?}    -Dmapred.map.output.compression.codec=\"org.apache.hadoop.io.compress.SnappyCodec\""
 pig_command="${pig_command?}    -Dpig.tmpfilecompression=true"
 pig_command="${pig_command?}    -Dpig.tmpfilecompression.codec=\"gz\""
 pig_command="${pig_command?}    -Doutput.compression.enabled=true"
 pig_command="${pig_command?}    -Doutput.compression.codec=\"org.apache.hadoop.io.compress.GzipCodec\""
 pig_command="${pig_command?}   -param_file ${param_file?}"
 pig_command="${pig_command?}   ${dry_run_flag?}"
 pig_command="${pig_command?}  fake_${type?}.pig"

 echo ${pig_command?}
 eval ${pig_command?}
 echo

 project_end=$(date +%s) && echo "fake: ${type?}: ${project_id?}: ellapsed time: $[project_end-project_start]"
done

# ===========================================================================

end=$(date +%s) && echo "fake: ${type?}: all: ellapsed time: $[end-start]"
date "+%y%m%d%H%M%S"

