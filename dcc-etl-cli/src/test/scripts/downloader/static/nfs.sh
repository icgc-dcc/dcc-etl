#!/bin/bash -e

run_name=${1?} && shift # load-prod-06e-32-22, load-prod-06e-40-23

run_name=load-prod-06e-41-26
run_number=11
type=stsm

mkdir /nfs/dcc_public/dcc/data/testing/${run_name?}
mkdir /nfs/dcc_public/dcc/data/testing/${run_name?}/${type?}

dir=/hdfs/dcc/icgc/testing/${run_name?}-${run_number?}
for project_id in $(ls -d ${dir?}/*.${type?} | awk -F$'/' '{print $NF}' | awk -F$'.' '{print $1}' | sort -u); do
 subdir=${dir?}/${project_id?}.${type?}
 {
  cat ${subdir?}/.pig_header | tr '\t' '\n' | awk -F$':' '{print $NF}' | tr '\n' '\t' | sed 's/\t$//' # get rid of qualifiers (qualifier1::qualifier2::field)
  echo
  cat ${subdir?}/part-*
 } > /nfs/dcc_public/dcc/data/testing/${run_name?}/${type?}/${project_id?}_${type?}.tsv
done


