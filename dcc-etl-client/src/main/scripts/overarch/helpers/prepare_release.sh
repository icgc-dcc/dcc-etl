#!/bin/bash
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

release_number='20'
dictionary_version='0.12e'
artifactory_release_url='http://seqwaremaven.oicr.on.ca/artifactory/simple/dcc-release/org/icgc/dcc/'

dcc_etl_cli_name='dcc-etl-client'
dcc_etl_annotator_name='dcc-etl-annotator'
dcc_etl_exporter_name='dcc-etl-exporter'
dcc_submission_server_name='dcc-submission-server'

dcc_etl_version='3.8.17.7'
dcc_etl_cli_version=${dcc_etl_version}
dcc_etl_annotator_version=${dcc_etl_version}
dcc_etl_exporter_version=${dcc_etl_version}
dcc_submission_server_version='3.8.6.4'

dcc_etl_cli_dist_file_name=${dcc_etl_cli_name}-${dcc_etl_cli_version}.jar
dcc_etl_annotator_dist_file_name=${dcc_etl_annotator_name}-${dcc_etl_annotator_version}.jar
dcc_submission_server_dist_file_name=${dcc_submission_server_name}-${dcc_submission_server_version}.jar

dcc_etl_exporter_dist_file_name=${dcc_etl_exporter_name}-${dcc_etl_exporter_version}-dist.tar.gz

# update libraries and their symbolic links
cd ***REMOVED***/dcc-etl/lib
wget ${artifactory_release_url}${dcc_etl_cli_name}/${dcc_etl_cli_version}/${dcc_etl_cli_dist_file_name}
ln -sfn ${dcc_etl_cli_dist_file_name} dcc-etl.jar

wget ${artifactory_release_url}${dcc_etl_annotator_name}/${dcc_etl_annotator_version}/${dcc_etl_annotator_dist_file_name}
ln -sfn ${dcc_etl_annotator_dist_file_name} dcc-annotator.jar

wget ${artifactory_release_url}dcc-submission-server/${dcc_submission_server_version}/${dcc_submission_server_dist_file_name}
ln -sfn ${dcc_submission_server_dist_file_name} dcc-validator.jar

# update db-importer(and run if required) and exporter distribution
~/dcc-import/bin/install -l

cd ***REMOVED***
wget ${artifactory_release_url}dcc-etl-exporter/${dcc_etl_exporter_version}/${dcc_etl_exporter_dist_file_name}
tar -xzf ${dcc_etl_exporter_dist_file_name}
rm ${dcc_etl_exporter_dist_file_name}
ln -sfn ${dcc_etl_exporter_name}-${dcc_etl_exporter_version} dcc-exporter

# update sources
cd ***REMOVED***/dcc-etl/git/dcc-etl
git pull

# update release number and dictionary version in shell scripts.
run_helper=***REMOVED***/dcc-etl/git/dcc-etl/dcc-etl-client/src/main/scripts/overarch/helpers/run.sh
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
