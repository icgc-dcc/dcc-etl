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

# Script that performs count/diff checks for ssm exported files

# ===========================================================================

mapping_file="/tmp/ssm_p_mapping.tsv"
rm ${mapping_file?} 2>&- || :
cat << EOF > ${mapping_file?}
ALL-US:/hdfs/dcc/icgc/submission/ICGC16
BLCA-CN:/hdfs/dcc/icgc/submission/ICGC16
BLCA-US:/hdfs/dcc/icgc/submission/ICGC16
BOCA-UK:/hdfs/dcc/icgc/submission/ICGC15
BRCA-UK:/nfs/dcc_secure/dcc/etl/icgc16/migration/all
BRCA-US:/hdfs/dcc/icgc/submission/ICGC16
CESC-US:/hdfs/dcc/icgc/submission/ICGC16
CLLE-ES:/hdfs/dcc/icgc/submission/ICGC16
CMDI-UK:/hdfs/dcc/icgc/submission/ICGC15
COAD-US:/hdfs/dcc/icgc/submission/ICGC16
EOPC-DE:/hdfs/dcc/icgc/submission/ICGC16
ESAD-UK:/hdfs/dcc/icgc/submission/ICGC15
ESCA-CN:/hdfs/dcc/icgc/submission/ICGC16
GACA-CN:/hdfs/dcc/icgc/submission/ICGC16
GBM-US:/hdfs/dcc/icgc/submission/ICGC16
HNSC-US:/hdfs/dcc/icgc/submission/ICGC16
KIRC-US:/hdfs/dcc/icgc/submission/ICGC16
KIRP-US:/hdfs/dcc/icgc/submission/ICGC16
LAML-KR:/nfs/dcc_secure/dcc/etl/icgc16/migration/all
LAML-US:/hdfs/dcc/icgc/submission/ICGC16
LGG-US:/hdfs/dcc/icgc/submission/ICGC16
LICA-FR:/hdfs/dcc/icgc/submission/ICGC16
LIHC-US:/hdfs/dcc/icgc/submission/ICGC16
LINC-JP:/hdfs/dcc/icgc/submission/ICGC15
LIRI-JP:/hdfs/dcc/icgc/submission/ICGC16
LUAD-US:/hdfs/dcc/icgc/submission/ICGC16
LUSC-KR:/nfs/dcc_secure/dcc/etl/icgc16/migration/all
LUSC-US:/hdfs/dcc/icgc/submission/ICGC16
MALY-DE:/hdfs/dcc/icgc/submission/ICGC16
NBL-US:/hdfs/dcc/icgc/submission/ICGC16
ORCA-IN:/hdfs/dcc/icgc/submission/ICGC16
OV-AU:/hdfs/dcc/icgc/submission/ICGC16
OV-US:/hdfs/dcc/icgc/submission/ICGC16
PAAD-US:/hdfs/dcc/icgc/submission/ICGC16
PACA-AU:/hdfs/dcc/icgc/submission/ICGC16
PACA-CA:/hdfs/dcc/icgc/submission/ICGC16
PAEN-AU:/hdfs/dcc/icgc/submission/ICGC16
PBCA-DE:/hdfs/dcc/icgc/submission/ICGC16
PRAD-CA:/hdfs/dcc/icgc/submission/ICGC15
PRAD-UK:/hdfs/dcc/icgc/submission/ICGC16
PRAD-US:/hdfs/dcc/icgc/submission/ICGC16
READ-US:/hdfs/dcc/icgc/submission/ICGC16
RECA-CN:/hdfs/dcc/icgc/submission/ICGC16
RECA-EU:/nfs/dcc_secure/dcc/etl/icgc16/migration/ssm_p
SKCM-US:/hdfs/dcc/icgc/submission/ICGC16
STAD-US:/hdfs/dcc/icgc/submission/ICGC16
THCA-SA:/hdfs/dcc/icgc/submission/ICGC15
THCA-US:/hdfs/dcc/icgc/submission/ICGC16
UCEC-US:/hdfs/dcc/icgc/submission/ICGC16
EOF

# ===========================================================================

# Removes trailing and leadings whitespaces
function trim() {
 awk -F$'\t' -v OFS='\t' '{gsub(/^ +/,"", $1);gsub(/ +$/,"", $1);gsub(/^ +/,"", $2);gsub(/ +$/,"", $2);gsub(/^ +/,"", $3);gsub(/ +$/,"", $3);gsub(/^ +/,"", $4);gsub(/ +$/,"", $4);gsub(/^ +/,"", $5);gsub(/ +$/,"", $5);gsub(/^ +/,"", $6);gsub(/ +$/,"", $6);gsub(/^ +/,"", $7);gsub(/ +$/,"", $7);gsub(/^ +/,"", $8);gsub(/ +$/,"", $8);gsub(/^ +/,"", $9);gsub(/ +$/,"", $9)}1'
}

# Translates mutation_type code to its string value
function translate() {
 field=${1?}
 awk -F$'\t' -v OFS="\t" '{gsub(/1/,"single base substitution",$'${field?}');gsub(/2/,"insertion of <=200bp",$'${field?}');gsub(/3/,"deletion of <=200bp",$'${field?}');gsub(/4/,"multiple base substitution (>=2bp and <=200bp)",$'${field?}')}1'
}

# Modifies genotype values (empty or non-empty); For original and controlled files, we need to normalize genotypes to present/absent, as in case of filtering we arbitrarily pick one among the originals
function normalize_genotype() {
 field=${1?} && shift
 empty_value=${1?} && shift
 nonempty_value=${1?} && shift
 awk -F$'\t' -v OFS="\t" '{gsub(/[^ ]+/,"'${empty_value?}'",$'${field?}');gsub(/^$/,"'${nonempty_value?}'",$'${field?}')}1'
}

function void_field() {
 field=${1?} && shift
 awk -F$'\t' -v OFS='\t' '{$'${field?}'="N/A"}1'
}

# ===========================================================================
# Initializations

wd="/tmp/check_ssm"
mkdir -p ${wd?}
touch ${wd?}/dummy
rm ${wd?}/* 2>&- || :

# ===========================================================================

# For each project, project/trim/translate/normalize fields of interest and count/diff results in terms of original/open/controlled version
# Chosen fields are: chromosome, chromosome_start, chromosome_end, mutation_type, genome_reference_allele, (control_genotype), (tumour_genotype), mutated_from_allele, mutated_to_allele (in that order)
for entry in $(cat ${mapping_file?}); do
 parent_dir=$(echo $entry | cut -d: -f2)
 project_key=$(echo $entry | cut -d: -f1)

 # ---------------------------------------------------------------------------
 # Original file(s)
 echo -ne "${project_key?}\toriginal\t" # There may be more than 1 such file and more than 1 format (see LICA-FR for instance)
 {
  pattern="${parent_dir?}/${project_key?}/ssm_p*.txt"
  if [ -n "$(ls ${pattern?} 2>&-)" ]; then
   for file in $(ls ${pattern?}); do { cat ${file?} | tail -n+2; echo; } | tr '\r' '\n' | awk '!/^$/'; done
  fi

  pattern="${parent_dir?}/${project_key?}/ssm_p*.gz"
  if [ -n "$(ls ${pattern?} 2>&-)" ]; then
   for file in $(ls ${pattern?}); do { gzip -cd ${file?} | tail -n+2; echo; } | tr '\r' '\n' | awk '!/^$/'; done
  fi

  pattern="${parent_dir?}/${project_key?}/ssm_p*.bz2"
  if [ -n "$(ls ${pattern?} 2>&-)" ]; then
   for file in $(ls ${pattern?}); do { bzip2 -cd ${file?} | tail -n+2; echo; } | tr '\r' '\n' | awk '!/^$/'; done
  fi
 } \
  | awk -F$'\t' '!/^$/{print $4 "\t" $5 "\t" $6 "\t" $3 "\t" $8 "\t" $9 "\t" $12 "\t" $10 "\t" $11}' \
  | trim | translate 4 | normalize_genotype 6 "present" "absent" | normalize_genotype 7 "present" "absent" | sort -u \
  > ${wd?}/${project_key?}.original
 wc -l ${wd?}/${project_key?}.original | awk '{print $1}'

 # ---------------------------------------------------------------------------
 # Controlled static download file
 echo -ne "${project_key?}\tcontrolled\t"
 static_file="/hdfs/dcc/tmp/download/static/release_16/Projects/${project_key?}/simple_somatic_mutation.controlled.${project_key?}.tsv.gz"
 if [ -f ${static_file?} ]; then
  zcat ${static_file?} | tail -n+2 | awk -F$'\t' '{print $9 "\t" $10 "\t" $11 "\t" $14 "\t" $15 "\t" $16 "\t" $17 "\t" $19 "\t" $20}' \
   | normalize_genotype 6 "present" "absent" | normalize_genotype 7 "present" "absent" | sort -u \
   > ${wd?}/${project_key?}.controlled
  wc -l ${wd?}/${project_key?}.controlled | awk '{print $1}'
 else
  echo 0
 fi

 # ---------------------------------------------------------------------------
 # Open static download file
 echo -ne "${project_key?}\topen    \t"
 static_file="/hdfs/dcc/tmp/download/static/release_16/Projects/${project_key?}/simple_somatic_mutation.open.${project_key?}.tsv.gz"
 if [ -f ${static_file?} ]; then
  zcat ${static_file?} | tail -n+2 | awk -F$'\t' '{print $9 "\t" $10 "\t" $11 "\t" $14 "\t" $15 "\t\t\t" $16 "\t" $17}' \
   | normalize_genotype 6 "present" "absent" | normalize_genotype 7 "present" "absent" | sort -u \
   > ${wd?}/${project_key?}.open
  wc -l ${wd?}/${project_key?}.open | awk '{print $1}'
 else
  echo 0
 fi
 echo

 # ---------------------------------------------------------------------------
 declare -i count=$(wc -l ${wd?}/${project_key?}.original | awk '{print $1}') && [ ${count?} -gt 0 ] && has_ssm=true || has_ssm=false
 if ${has_ssm?}; then

  # ---------------------------------------------------------------------------
  # Diff original/controlled
  echo -ne "original VS controlled:\t"
  diff -q ${wd?}/${project_key?}.original ${wd?}/${project_key?}.controlled \
   && echo "OK" || echo "KO"

  # ---------------------------------------------------------------------------
  # Diff original/open; We cannot include the genotypes in the comparison as they are never included in the OPEN files; Likewise we need to ignore the mutated_from_allele since it may have been masked
  echo -ne "original VS open:\t"
  diff -q \
   <(cat ${wd?}/${project_key?}.original | normalize_genotype 6 "" "" | normalize_genotype 7 "" "" | void_field 8 | sort -u) \
   <(cat ${wd?}/${project_key?}.open | normalize_genotype 6 "" "" | normalize_genotype 7 "" "" | void_field 8 | sort -u) \
   && echo "OK" || echo "KO"
  echo
 else
  echo "no ssm"
  echo
 fi
 echo

done
echo

echo done

