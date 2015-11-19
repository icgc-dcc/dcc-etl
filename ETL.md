ICGC DCC - Running ETL
===

The steps required to prepare and run the ETL pipeline. 


### 1. Preparing for run.

#### 1.0. Connect.

Login to ***REMOVED*** and change user to dcc_dev

```bash
ssh ***REMOVED***
sudo -u dcc_dev -i
```

start a tmux session | attach to an existing one.
```bash
tmux new -s etl | tmux attach -t etl
```
#### 1.0. Update Dependencies.

There is a [shell script](https://github.com/icgc-dcc/dcc-etl/blob/develop/dcc-etl-client/src/main/scripts/overarch/helpers/prepare_release.sh) that encapsulates most of the process for updating the dependencies to ETL run. However, there are manual steps involved that could not be automated at the time. It is also recommended to run the commands from the mentioned script individually, instead of running the shell script, to allow for verification of steps, which are described in more detail below.

#### 1.0.1 Define variables.

In order to make the process easier and avoid error, we define some variables describing the settings.

```bash
release_number='19'
dictionary_version='0.11c'
artifactory_release_url='http://seqwaremaven.oicr.on.ca/artifactory/simple/dcc-release/org/icgc/dcc/'

dcc_etl_cli_name='dcc-etl-client'
dcc_etl_annotator_name='dcc-etl-annotator'
dcc_etl_exporter_name='dcc-etl-exporter'
dcc_submission_server_name='dcc-submission-server'

dcc_etl_version='3.8.5.4'
dcc_etl_cli_version=${dcc_etl_version}
dcc_etl_annotator_version=${dcc_etl_version}
dcc_etl_exporter_version=${dcc_etl_version}
dcc_submission_server_version='3.8.6.2'

dcc_etl_cli_dist_file_name=${dcc_etl_cli_name}-${dcc_etl_cli_version}.jar
dcc_etl_annotator_dist_file_name=${dcc_etl_annotator_name}-${dcc_etl_annotator_version}.jar
dcc_submission_server_dist_file_name=${dcc_submission_server_name}-${dcc_submission_server_version}.jar

dcc_etl_exporter_dist_file_name=${dcc_etl_exporter_name}-${dcc_etl_exporter_version}-dist.tar.gz
```

#### 1.2. Update libraries and their symbolic links

```bash
cd ***REMOVED***/dcc-etl/lib
wget ${artifactory_release_url}${dcc_etl_cli_name}/${dcc_etl_cli_version}/${dcc_etl_cli_dist_file_name}
ln -sfn ${dcc_etl_cli_dist_file_name} dcc-etl.jar

wget ${artifactory_release_url}${dcc_etl_annotator_name}/${dcc_etl_annotator_version}/${dcc_etl_annotator_dist_file_name}
ln -sfn ${dcc_etl_annotator_dist_file_name} dcc-annotator.jar

wget ${artifactory_release_url}dcc-submission-server/${dcc_submission_server_version}/${dcc_submission_server_dist_file_name}
ln -sfn ${dcc_submission_server_dist_file_name} dcc-validator.jar
```

#### 1.3. Update db-importer(and run if required) and exporter distribution

```bash
~/dcc-import/bin/install -l

cd ***REMOVED***
wget ${artifactory_release_url}dcc-etl-exporter/${dcc_etl_exporter_version}/${dcc_etl_exporter_dist_file_name}
tar -xzf ${dcc_etl_exporter_dist_file_name}
rm ${dcc_etl_exporter_dist_file_name}
ln -sfn ${dcc_etl_exporter_name}-${dcc_etl_exporter_version} dcc-exporter
```

#### 1.4. Update sources

```bash
cd ***REMOVED***/dcc-etl/git/dcc-etl
git pull
```

#### 1.5. Update release number and dictionary version in shell scripts.

```bash
run_helper=***REMOVED***/dcc-etl/git/dcc-etl/dcc-etl-client/src/main/scripts/overarch/helpers/run.sh
sed -i "s/^release_number=.*/release_number=${release_number}/" ${run_helper}
sed -i "s/^dictionary_version=.*/dictionary_version=\"${dictionary_version}\"/" ${run_helper}
git add ${run_helper}
git commit -m "Updated release number and dictionary version."
git push
```

#### 1.6. Update config files

```bash
cd ***REMOVED***/dcc-etl/conf/
```

#### 1.7. Update `etl_prod.yml` if needed

#### 1.8. Update dictionary.json and codelists.json from production server

```bash
dictionary_file=dictionaries/${dictionary_version}.json
curl -v -XGET http://***REMOVED***/ws/nextRelease/dictionary -H "Accept: application/json" > ${dictionary_file}
curl -v -XGET http://***REMOVED***/ws/codeLists -H "Accept: application/json" > codelists.json
```

#### 1.9. Push possible changes

```bash
git status
# if there are changes, commit and push them.
git add codelists.json
git add ${dictionary_file}
git commit -m "Updated dictionary and codelist configuration files."
git push
```

#### 1.10. Create projects.json for release, if required
Currently there is no reliable way to automate this step. Get the formal list of projects to be included and ensure each project exists in [ICGC.org](https://icgc.org/icgc)
use the pre-existing project.json in `***REMOVED***/dcc-etl/conf/projects` as a template to create a new one with the project names and commit the new file.

```bash
cd ***REMOVED***/dcc-etl/conf/projects
touch icgc${release_number}.json 
git add . && git commit -m "Added project.json configuration file." && git push
```

### 2. Run

First, if needed, run db-importer to update MongoDB database containing projects, go, genes, CGC and pathways data.

```bash
cd ***REMOVED***/dcc-etl-db-importer
bin/db-importer.sh
```

Change to the directory containing the overarching shell script.

```bash
cd ***REMOVED***/dcc-etl/git/dcc-etl/dcc-etl-client/src/main/scripts/overarch
```

Use the following command to run the ETL

```bash
helpers/run.sh ***REMOVED***/dcc-etl/git/dcc-etl dcc-etl.jar <project>.json <test|ICGC> <component_name>-<component_name>
```

where `<project>.json` is the file indicating the project to be included in the run, `<test|ICGC>` indicates if the run is a test or release and `<component_name>-<component_name>` specifies the starting and ending components in the run. To run all, use `-`. For more advanced settings see the [shell script.](https://github.com/icgc-dcc/dcc-etl/blob/develop/dcc-etl-client/src/main/scripts/overarch/overarch.sh#L43)

For example, the following command will execute ETL in test mode, using two_tcga.json project file, and will run the components including and after loader.

```bash
helpers/run.sh ***REMOVED***/dcc-etl/git/dcc-etl dcc-etl.jar two_small_tcga.json test loader-
```

### 3. Monitor

Logs are under jobs folder `***REMOVED***/dcc-etl/jobs/releases`. For example, `***REMOVED***/dcc-etl/jobs/releases/test19/patches/0/runs/3/attempts/2/logs/loader.log`. Following the highest number in each directoty will guide the user to the logs from latest run.

### 4. Manual deployment
In order to run ETL using a snapshot build from local machine, use the following snippet, replacing versions and paths as needed.

Build dcc-etl distribution modules and upload them.

```bash
# dcc-etl project
cd dcc-etl 
mvn clean package
scp dcc-etl-client/target/dcc-etl-client-3.8.5.5-SNAPSHOT.jar ***REMOVED***:~
scp dcc-etl-annotator/target/dcc-etl-annotator-3.8.5.5-SNAPSHOT.jar ***REMOVED***:~
scp dcc-etl-exporter/target/dcc-etl-exporter-3.8.5.5-SNAPSHOT-dist.tar.gz ***REMOVED***:~
```

Build dcc-submission-server distribution and upload them.

```bash
# dcc-submission project
cd dcc-submission 
mvn clean package
scp dcc-submission-server/target/dcc-submission-server-3.8.6.2-SNAPSHOT.jar ***REMOVED***:~
```

Follow the procedure similar to steps 1.2 and 1.3 to get the files in their proper place.

```bash
ssh hproxy-dcc
sudo -u dcc_dev -i
cp /u/user_name/dcc-etl-client-3.8.5.5-SNAPSHOT.jar ***REMOVED***/dcc-etl/lib/
cd ***REMOVED***/dcc-etl/lib/
ln -sfn dcc-etl-client-3.8.5.5-SNAPSHOT.jar dcc-etl.jar
```
And so on for the rest of the uploaded files.

### Additional Resources
[Job Tracker](http://***REMOVED***/jobtracker.jsp)

When a job id is shown in error logs, use the following link to see the relevant information: http://***REMOVED***/jobdetails.jsp?jobid=[job_id]

[Ganglia] (http://***REMOVED***/ganglia/)
