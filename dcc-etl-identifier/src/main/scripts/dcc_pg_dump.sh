#!/bin/bash

declare -a tablenames=("donor_ids" "mutation_ids" "project_ids" "sample_ids" "specimen_ids")

if [ $# -eq 1 ] ; then
outputDir=$1

## now loop through the above array
for tablename in "${tablenames[@]}"
do
outputFile=${outputDir}/${tablename}
echo "dumping ${tablename} to ${outputFile} ..."
psql -U dcc -h localhost dcc_identifier -c "COPY (select * from ${tablename}) TO STDOUT WITH DELIMITER as E'\t' CSV HEADER" > ${outputFile}
done
else
echo "missing output directory"
echo "$0 <output directory>"
fi