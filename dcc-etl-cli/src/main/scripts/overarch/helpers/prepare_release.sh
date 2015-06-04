#!/bin/bash
release_number='19'
dictionary_version='0.11c'
artifactory_release_url='http://seqwaremaven.oicr.on.ca/artifactory/simple/dcc-release/org/icgc/dcc/'

dcc_etl_cli_name='dcc-etl-cli'
dcc_etl_annotator_name='dcc-etl-annotator'
dcc_etl_exporter_name='dcc-etl-exporter'
dcc_etl_db_importer_name='dcc-etl-db-importer'
dcc_submission_server_name='dcc-submission-server'

dcc_etl_cli_version='3.8.8.1'
dcc_etl_annotator_version='3.8.8.1'
dcc_etl_exporter_version='3.8.8.1'
dcc_etl_db_importer_version='3.8.8.1'
dcc_submission_server_version='3.8.6.4'

dcc_etl_cli_dist_file_name=${dcc_etl_cli_name}-${dcc_etl_cli_version}.jar
dcc_etl_annotator_dist_file_name=${dcc_etl_annotator_name}-${dcc_etl_annotator_version}.jar
dcc_submission_server_dist_file_name=${dcc_submission_server_name}-${dcc_submission_server_version}.jar

dcc_etl_exporter_dist_file_name=${dcc_etl_exporter_name}-${dcc_etl_exporter_version}-dist.tar.gz
dcc_etl_db_importer_dist_file_name=${dcc_etl_db_importer_name}-${dcc_etl_db_importer_version}-dist.tar.gz

# update libraries and their symbolic links
cd ***REMOVED***/dcc-etl/lib
wget ${artifactory_release_url}${dcc_etl_cli_name}/${dcc_etl_cli_version}/${dcc_etl_cli_dist_file_name}
ln -sfn ${dcc_etl_cli_dist_file_name} dcc-etl.jar

wget ${artifactory_release_url}${dcc_etl_annotator_name}/${dcc_etl_annotator_version}/${dcc_etl_annotator_dist_file_name}
ln -sfn ${dcc_etl_annotator_dist_file_name} dcc-annotator.jar

wget ${artifactory_release_url}dcc-submission-server/${dcc_submission_server_version}/${dcc_submission_server_dist_file_name}
ln -sfn ${dcc_submission_server_dist_file_name} dcc-validator.jar

# update db-importer and exporter distribution
cd ***REMOVED***

wget ${artifactory_release_url}${dcc_etl_db_importer_name}/${dcc_etl_db_importer_version}/${dcc_etl_db_importer_dist_file_name}
tar -xzf ${dcc_etl_db_importer_dist_file_name}
rm ${dcc_etl_db_importer_dist_file_name}
cp dcc-etl-db-importer/conf/* ${dcc_etl_db_importer_name}-${dcc_etl_db_importer_version}/conf
ln -sfn ${dcc_etl_db_importer_name}-${dcc_etl_db_importer_version} dcc-etl-db-importer

wget ${artifactory_release_url}dcc-etl-exporter/${dcc_etl_exporter_version}/${dcc_etl_exporter_dist_file_name}
tar -xzf ${dcc_etl_exporter_dist_file_name}
rm ${dcc_etl_exporter_dist_file_name}
ln -sfn ${dcc_etl_exporter_name}-${dcc_etl_exporter_version} dcc-exporter

# update sources
cd ***REMOVED***/dcc-etl/git/dcc-etl
git pull

# update release number and dictionary version in shell scripts.
run_helper=***REMOVED***/dcc-etl/git/dcc-etl/dcc-etl-cli/src/main/scripts/overarch/helpers/run.sh
sed -i "s/^release_number=.*/release_number=${release_number}/" ${run_helper}
sed -i "s/^dictionary_version=.*/dictionary_version=\"${dictionary_version}\"/" ${run_helper}
git add ${run_helper}
git commit -m "Updated release number and dictionary version."
git push

# update config files
cd ***REMOVED***/dcc-etl/conf/

# update etl_prod.yml if needed

# update dictionary.json from production server
dictionary_file=dictionaries/${dictionary_version}.json
curl -v -XGET http://***REMOVED***/ws/nextRelease/dictionary -H "Accept: application/json" > ${dictionary_file}

# update codelists.json from production server
curl -v -XGET http://***REMOVED***/ws/codeLists -H "Accept: application/json" > codelists.json

git add codelists.json
git add ${dictionary_file}
git commit -m "Updated dictionary and codelist configuration files."
git push

# create projects.json for release, currently there is no reliable way to automate this step.
# get the formal list of projects to be included in and ensure each project exists in ICGC.org (https://icgc.org/icgc)
# use the pre-existing project.json in ***REMOVED***/dcc-etl/conf/projects as a template 
# to create a new one with the project names and commit the new file.
# cd ***REMOVED***/dcc-etl/conf/projects
# touch icgc${release_number}.json 
# git add . && git commit -m "Added project.json configuration file." && git push
