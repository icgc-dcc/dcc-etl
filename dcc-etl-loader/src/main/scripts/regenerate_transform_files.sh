#!/bin/bash -e
# usage: must be in loader dir, must provide path to dictionary file
# example: data-submission$ ./loader/src/main/scripts/regenerate_transform_files.sh $PWD/server/src/main/resources/0.6c.CLOSED.json

[ "$(basename $PWD)" == "data-submission" ] || { echo "ERROR: must be in data-submission dir"; exit 1; }
dictionary_file=${1?}

file_pattern="loader/target/classes/transform/*.gen.transform.json"
rm ${file_pattern?} 2>&- || :

cd loader
mvn exec:java -Dexec.mainClass="org.icgc.dcc.submission.etl.loader.generator.Main" -Dexec.args="${dictionary_file?}"
cd ..

for file in $(ls -1 ${file_pattern?}); do
 filename=$(basename ${file?} | sed 's/\.gen//g')
 loader/src/main/scripts/format.sh ${file?} > loader/src/main/resources/transform/${filename?}
done
echo
git status --porcelain loader/src/main/resources/transform
echo
