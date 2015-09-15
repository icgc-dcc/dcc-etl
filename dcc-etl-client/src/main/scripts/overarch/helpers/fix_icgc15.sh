#!/bin/bash

################################################################################
# Fix validated data due to inconsistencies in submission and ETL.
# ICGC15 only !!
# 
# Usage
#   ./fix_icgc15 <path_to_project_directory>
#   
#   ./fix_icgc15 /nfs/dcc_secure/dcc/data/ICGC15/DCC-1925/testdata/BRCA-US
#
# 1) Rename normalizer to normalization
# 2) Rename ssm__p.txt to ssm_p.txto
# 3) Change masking to marking for part file header
################################################################################


path_to_project=${1?} && shift

echo "Scanning ${path_to_project?}"
if [ ! -d ${path_to_project?} ]; then
   echo "cannot find ${path_to_project?}"
   exit 0
fi

if [ ! -d ${path_to_project?}/.validation ]; then
   echo ".validation folder does not exist, no change"
   exit 0
fi


echo "1. normalizer ==> normalization"
current=${path_to_project?}/.validation
if [ -d ${current?}/normalizer ]; then
   echo "Change normalization to normalizer"
   mv -v ${current?}/normalizer ${current?}/normalization
fi


echo "2. ssm__p.txt ==> ssm_p.txt"
current=${path_to_project?}/.validation/normalization/data
if [ -d ${current?}/ssm__p.txt ]; then
   echo "Change ssm__p.txt to ssm_p.txt"
   mv -v ${current?}/ssm__p.txt ${current?}/ssm_p.txt
fi


echo "3. marking to masking"
current=${path_to_project?}/.validation/normalization/data/ssm_p.txt
for part in `ls ${current?}/part-*`; do
   echo "    Fixing ${part?}"
   perl -i -pe 's/masking/marking/g if 1 .. 1' ${part?}   
done
