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


#TODO: find a way to check we can't get MORE types than available

destination_dir=${1?} && shift # somewhere with a lot of disk space
result_file=${1?}

# ===========================================================================

#function encode() { echo $* | sed 's/"/%22/g'; }
function merge() { ls ${1?}/${2?}.DO*.${3?}.tsv | head -n1 | xargs head -n1; tail -n+2 -q ${1?}/${2?}.DO*.${3?}.tsv && rm ${1?}/${2?}.DO*.${3?}.tsv; }
static_dir=/hdfs/dcc/icgc/download/static/release_14

# ---------------------------------------------------------------------------

#projects="ALL-US"
#projects="ALL-US BLCA-US BRCA-US CESC-US CLLE-ES COAD-US EOPC-DE ESAD-UK GBM-US HNSC-US KIRC-US KIRP-US LAML-US LGG-US LICA-FR LIHC-US LINC-JP LIRI-JP LUAD-US LUSC-US MALY-DE OV-US PAAD-US PACA-AU PRAD-CA PACA-CA PBCA-DE PRAD-US READ-US SKCM-US STAD-US THCA-US UCEC-US"
projects=$(ls ${static_dir?})

protocol="http"
host="<proxy-prod>" # private instance without upper limit
port="5381"

mkdir -p ${destination_dir?}
for project in ${projects?}; do
 printf '=%.0s' {1..75} && echo
 echo ${project?}
 echo

 # ---------------------------------------------------------------------------
 ## static files:

 files=$(ls $static_dir/${project?}/*)

 types=$(
python <<eof
import re
types = []
files = "$(echo ${files?} | tr '\n' ',')".split(',')
for file in files:
 if re.search("\/clinical\.", file):
  types.append("clinical")
 if re.search("\/clinicalsample\.", file):
  types.append("sample")
 if re.search("\/simple_somatic_mutation\.", file):
  types.append("ssm")
 if re.search("\/copy_number_somatic_mutation\.", file):
  types.append("cnsm")
 if re.search("\/structural_somatic_mutation\.", file):
  types.append("stsm")
 if re.search("\/methylation\.", file):
  types.append("meth")
 if re.search("\/mirna_expression\.", file):
  types.append("mirna")
 if re.search("\/gene_expression\.", file):
  types.append("exp")
 if re.search("\/protein_expression\.", file):
  types.append("pexp")
 if re.search("\/splice_variant\.", file):
  types.append("jcn")

assert "clinical" in types, "Couldn't find clinical file"
assert "sample" in types, "Couldn't find sample file"

#if "meth" in types:
# types.remove("meth")
#if "exp" in types:
# types.remove("exp")

#if "ssm" in types:
# types.remove("ssm")
#if "cnsm" in types:
# types.remove("cnsm")
#if "stsm" in types:
# types.remove("stsm")
#if "mirna" in types:
# types.remove("mirna")
#if "pexp" in types:
# types.remove("pexp")
#if "jcn" in types:
# types.remove("jcn")

print ' '.join(types)
eof
)
 echo "types=\"${types?}\""

 # ---------------------------------------------------------------------------
 ## dynamic files:
 mkdir ${destination_dir?}/${project?} ${destination_dir?}/${project?}_tmp

 # create filter for project
 #filters=$(encode '{"project":{"_project_id":["'${project?}'"]}}')
 filters='{"project":{"_project_id":["'${project?}'"]}}'

 # create array type
 types_array=""
 for type in ${types?}; do
  if [ "${type?}" != "sample" ]; then # must not include it, it's implicit with clinical
   types_array="${types_array?},"'{"key":"'${type?}'","value":"TSV"}'
  fi
 done
 types_array=$(echo "${types_array?}" | sed 's/^,//')
 #info=$(encode '['${types_array?}']')
 info='['${types_array?}']'

 url="${protocol?}://${host?}:${port?}/api/download?filters=${filters?}&info=${info?}"

 # download the dynamic data
 file="${destination_dir?}/${project?}.dynamic.tar.gz"
 echo "wget '${url?}' -O ${file?}"
 wget "${url?}" -O ${file?}
 sleep 1

 # untar
 tar -C ${destination_dir?}/${project?}_tmp -zxf ${destination_dir?}/${project?}.dynamic.tar.gz 2>&1 | grep -v future || : # ignore "in the future" message
 rm ${destination_dir?}/${project?}.dynamic.tar.gz
 ls ${destination_dir?}/${project?}_tmp/* | head # To glance at the files

 # merge donor files
 for type in ${types?}; do
  echo "Merging ${project?}.${type?}"
  merge ${destination_dir?}/${project?}_tmp ${project?} ${type?} > ${destination_dir?}/${project?}/${project?}.${type?}.tsv
 done
 rm ${destination_dir?}/${project?}_tmp/README.txt
 rmdir ${destination_dir?}/${project?}_tmp

 echo
 # ---------------------------------------------------------------------------
 # count lines in dynamic files
 unset dynamic && declare -A dynamic
 for file in $(ls ${destination_dir?}/${project?}/*); do
  type=$(echo ${file?} | sed -r 's/.*\/.+\.(.+)\.tsv/\1/g')

  #if [ "${type?}" == "ssm" ]; then continue; fi
  #if [ "${type?}" == "cnsm" ]; then continue; fi
  #if [ "${type?}" == "stsm" ]; then continue; fi
  #if [ "${type?}" == "mirna" ]; then continue; fi
  #if [ "${type?}" == "pexp" ]; then continue; fi
  #if [ "${type?}" == "jcn" ]; then continue; fi

  #if [ "${type?}" == "meth" ]; then continue; fi
  #if [ "${type?}" == "exp" ]; then continue; fi

  count=$(wc -l ${file?} | awk '{print $1}')
  dynamic[${type?}]=${count?}
 done

 # ---------------------------------------------------------------------------
 # count lines in static files
 unset static && declare -A static
 for file in ${files?}; do

  if [[ "${file?}" =~ clinical\. ]]; then type="clinical"; fi
  if [[ "${file?}" =~ clinicalsample\. ]]; then type="sample"; fi

  if [[ "${file?}" =~ simple_somatic_mutation\. ]]; then type="ssm"; fi
  if [[ "${file?}" =~ copy_number_somatic_mutation\. ]]; then type="cnsm"; fi
  if [[ "${file?}" =~ structural_somatic_mutation\. ]]; then type="stsm"; fi
  if [[ "${file?}" =~ mirna_expression\. ]]; then type="mirna"; fi
  if [[ "${file?}" =~ protein_expression\. ]]; then type="pexp"; fi
  if [[ "${file?}" =~ splice_variant\. ]]; then type="jcn"; fi
  if [[ "${file?}" =~ gene_expression\. ]]; then type="exp"; fi
  if [[ "${file?}" =~ methylation\. ]]; then type="meth"; fi

  #if [[ "${file?}" =~ gene_expression\. ]]; then continue; fi
  #if [[ "${file?}" =~ methylation\. ]]; then continue; fi

  #if [[ "${file?}" =~ simple_somatic_mutation\. ]]; then continue; fi
  #if [[ "${file?}" =~ copy_number_somatic_mutation\. ]]; then continue; fi
  #if [[ "${file?}" =~ structural_somatic_mutation\. ]]; then continue; fi
  #if [[ "${file?}" =~ mirna_expression\. ]]; then continue; fi
  #if [[ "${file?}" =~ protein_expression\. ]]; then continue; fi
  #if [[ "${file?}" =~ splice_variant\. ]]; then continue; fi

  #if [[ "${file?}" =~ gene_expression\. ]]; then type="exp"; fi
  #if [[ "${file?}" =~ methylation\. ]]; then type="meth"; fi

  count=$(zcat ${file?} | wc -l | awk '{print $1}')
  static[${type?}]=${count?}
 done

 # ---------------------------------------------------------------------------
 # diff
 for type in ${types?}; do
  echo -ne "${project?}\t${type?}\t${static[${type?}]}\t${dynamic[${type?}]}\t" >> ${result_file?}
  { [ "${static[${type?}]}" == "${dynamic[${type?}]}" ] && echo OK || echo 'KO...'; } >> ${result_file?}
 done

 cat ${result_file?}

done

# ===========================================================================

