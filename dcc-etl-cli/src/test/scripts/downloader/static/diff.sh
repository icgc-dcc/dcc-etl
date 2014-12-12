#!/bin/bash -e

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

