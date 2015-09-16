#!/bin/bash -e

run_number=${1?} && shift
type=${1?} && shift

run_name=load-prod-06e-41

./submission.sh ${run_number?} ${type?}

ssm_projects="$(find /hdfs/dcc/icgc/normalizer/${run_name?} | grep ssm | awk -F$'/' '{print $7}' | sort -u)"
project_ids=$(find /hdfs/dcc/icgc/normalizer/${run_name?}-${run_number?} | awk '/\/'${type?}'/' | awk -F$'/' '{print $(NF-1)}' | sort -u | tr '\n' ' ')

for project_id in ${ssm_projects?}; do
 printf '=%.0s' {1..75} && echo && echo ${project_id?}
 pig -param run_name=${run_name?} -param run_number=${run_number?} -param project_id=${project_id?} normalizer.pig
 echo
done

for project_id in ${ssm_projects?}; do
 echo -n ${project_id?}
 [ -z "$(diff -q <(cat /hdfs/dcc/icgc/testing/diff/${project_id?}.original/part-*) <(cat /hdfs/dcc/icgc/testing/diff/${project_id?}.static/part-*))" ] && echo ": OK" || echo ": KO..."
done

echo OK

