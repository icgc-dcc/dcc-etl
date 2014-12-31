#!/bin/bash
release_number='18'
dictionary_version='0.10a'
artifactory_release_url='http://seqwaremaven.oicr.on.ca/artifactory/simple/dcc-release/org/icgc/dcc/'
dcc_etl_cli_version='3.7.5.1'
dcc_etl_annotator_version='3.7.5.1'
dcc_submission_server_version='3.7.4.1'

# update libraries and their symlinks
cd ***REMOVED***/dcc-etl/lib
wget ${artifactory_release_url}dcc-etl-cli/${dcc_etl_cli_version}/dcc-etl-cli-${dcc_etl_cli_version}.jar
wget ${artifactory_release_url}dcc-etl-annotator/${dcc_etl_annotator_version}/dcc-etl-annotator-${dcc_etl_annotator_version}.jar
wget ${artifactory_release_url}dcc-submission-server/${dcc_submission_server_version}/dcc-submission-server-${dcc_submission_server_version}.jar
ln -sfn dcc-etl-cli-${dcc_etl_cli_version}.jar dcc-etl.jar
ln -sfn dcc-etl-annotator-${dcc_etl_annotator_version}.jar dcc-annotator.jar
ln -sfn dcc-submission-server-${dcc_submission_server_version}.jar dcc-validator.jar

# update sources
cd ***REMOVED***/dcc-etl/git/dcc-etl
git pull

# update release number and dictionary version in shell scripts.
run_helper=***REMOVED***/dcc-etl/git/dcc-etl/dcc-etl-cli/src/main/scripts/overarch/helpers/run.sh
sed -i "s/^release_number=.*/release_number=${release_number}/" ${run_helper}
sed -i "s/^dictionary_version=.*/dictionary_version=\"${dictionary_version}\"/" ${run_helper}
git add .
git commit -m "Updated release number and dictionary version."
git push

# update config files
cd ***REMOVED***/dcc-etl/conf/

# update etl_prod.yml if needed

# update dictionary.json from production server
curl -v -XGET http://hwww2-dcc:5380/ws/nextRelease/dictionary -H "Accept: application/json" > dictionaries/${dictionary_version}.json

# update codelists.json from production server
curl -v -XGET http://hwww2-dcc:5380/ws/codeLists -H "Accept: application/json" > codelists.json

git add .
git commit -m "Updated dictionary and codelists configuration files."
git push

# create projects.json for release, currently there is no reliable way to automate this step.
# get the formal list of projects to be included in and ensure each project exists in ICGC.org (https://icgc.org/icgc)
# use the pre-existing project.json in ***REMOVED***/dcc-etl/conf/projects as a template 
# to create a new one with the project names and commit the new file.
# cd ***REMOVED***/dcc-etl/conf/projects
# touch icgc${release_number}.json 
# git add . && git commit -m "Added project.json configuration file." && git push
