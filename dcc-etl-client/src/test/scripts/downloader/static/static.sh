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


export HADOOP_USER_NAME=hdfs
date "+%y%m%d%H%M%S"
start=$(date +%s)

etl_run_name=${1?} && shift
etl_run_number=${1?} && shift
test_run_number=${1?} && shift
type=${1?} && shift
fake_input_dir=${1?} && shift
static_input_dir=${1?} && shift
project_ids=${1?} && shift # as CSV
default_parallel=${1?} && shift
child_memory=${1?} && shift
dry_run=$1 # anything non-empty

# ===========================================================================

function list_static_headers() { zcat ${1?} | head -n1 | tr '\t' '\n'; }
function list_fake_headers() { head -n1 ${1?} | tr '\t' '\n' | awk -F$':' '{print $NF}' | tr '\n' '\t' | tr '\t' '\n'; }

# ===========================================================================

unset long_types && declare -A long_types
long_types["ssm"]="simple_somatic_mutation"
long_types["cnsm"]="copy_number_somatic_mutation"
long_types["stsm"]="structural_somatic_mutation"
long_types["meth"]="methylation"
long_types["mirna"]="mirna_expression"
long_types["exp"]="gene_expression"
long_types["pexp"]="protein_expression"
long_types["jcn"]="splice_variant"

# ===========================================================================

for project_id in $(echo ${project_ids?} | tr ',' ' '); do
 printf '=%.0s' {1..75} && echo && echo "static: ${project_id?}"
 project_start=$(date +%s)
 static_input_file="${static_input_dir?}/${project_id?}/${long_types[${type?}]}.${project_id?}.tsv.gz"
 fake_header_file="/icgc/testing/fake/${etl_run_name?}-${etl_run_number?}-${test_run_number?}/${project_id?}.${type?}/.pig_header"

 echo
 list_static_headers /hdfs/dcc${static_input_file?} | sort
 echo
 list_fake_headers /hdfs/dcc${fake_header_file?} | sort
 echo

 declare -i expected_count=$(list_fake_headers /hdfs/dcc${fake_header_file?} | wc -l | awk '{print $1}')
 fields=$(join -t$'\t' -j 1 \
  <(list_static_headers /hdfs/dcc${static_input_file?} \
     | awk '{print $0 "\t" (NR-1)}' \
     | sort -t$'\t' -k1,1) \
  <(list_fake_headers /hdfs/dcc${fake_header_file?} \
     | awk '{print $0 "\t" (NR-1)}' \
     | sort -t$'\t' -k1,1) \
  | sort -t$'\t' -k2n,2n | awk -F$'\t' '{print "(chararray)$" $2 " AS " $1 ":chararray"}' | tr '\n' ',' | sed 's/,$//' | sed 's/,/, /g') # $31 AS gene_affected_by_bkpt_from, $32 AS gene_affected_by_bkpt_to, $8 AS placement, $1 AS project_code, $7 AS sv_id
 declare -i actual_count=$(echo ${fields?} | tr ',' '\n' | wc -l | awk '{print $1}')
 echo "fields=\"${fields?}\""
 [ ${expected_count?} == ${actual_count?} ] && echo "${expected_count?} == ${actual_count?}" || { echo "ERROR: ${expected_count?} != ${actual_count?}"; exit 1; }

 output_dir="/icgc/testing/static/${etl_run_name?}-${etl_run_number?}-${test_run_number?}/${project_id?}.${type?}"
 pig_script=$(mktemp)
 echo "set default_parallel ${default_parallel?}" >> ${pig_script?}
 echo "static = LOAD '${static_input_file?}';" >> ${pig_script?}
 echo "static = FOREACH static GENERATE ${fields?};" >> ${pig_script?}
 echo "static = FILTER static BY icgc_donor_id != 'icgc_donor_id';" >> ${pig_script?}
 echo "static = DISTINCT static;" >> ${pig_script?}
 echo "static = ORDER static BY *;" >> ${pig_script?}
 echo "STORE static INTO '${output_dir?}';" >> ${pig_script?}
 cat ${pig_script?}

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
 pig_command="${pig_command?}  ${dry_run_flag?}"
 pig_command="${pig_command?}  ${pig_script?}"

 echo ${pig_command?}
 eval ${pig_command?} && rm ${pig_script?}
 echo

 project_end=$(date +%s) && echo "static: ${type?}: ${project_id?}: ellapsed time: $[project_end-project_start]"
 echo OK

done

# ===========================================================================

end=$(date +%s) && echo "static: ${type?}: all: ellapsed time: $[end-start]"
date "+%y%m%d%H%M%S"

