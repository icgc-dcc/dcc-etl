#!/bin/bash
# Temporary script to help run the overarching script in different clusters (until it stabilizes)
# usage: helpers/run.sh myrelease myjar myprojects > mylogs
set -o nounset

# ===========================================================================

git_dir=${1?} && shift # ***REMOVED***/dcc-etl/git/dcc-etl
jar_file_name=${1?} && shift
projects_file_name=${1?} && shift
release_prefix=${1?} && shift
components_to_run=${1?} && shift

# ===========================================================================

host=$(hostname -f)
valid_host=false

if [ "$host" == "***REMOVED***" ]; then
 cluster="prod"
 use_docker=false
 valid_host=true
fi

if [ "$host" == "hcn-135.res.oicr.on.ca" ]; then
 cluster="prod"
 use_docker=true
 valid_host=true
fi

if [ "$host" == "***REMOVED***" ]; then
 cluster="prod"
 use_docker=false
 valid_host=true
fi

if [ "$host" == "***REMOVED***" ]; then
 cluster="dev"
 use_docker=false
 valid_host=true
fi

if [ "$host" == "dcc-etl-main" ]; then
 cluster="dev"
 use_docker=false
 valid_host=true
fi

if ! $valid_host; then
 echo "ERROR: invalid host: ${host?}"
 exit 1
fi

# ===========================================================================

overarch_dir=dcc-etl-cli/src/main/scripts/overarch
real_etl_dir=***REMOVED***/dcc-etl

if $use_docker; then
 etl_dir="/etl"
else
 etl_dir=${real_etl_dir?}
fi

real_jar_file=${real_etl_dir?}/lib/${jar_file_name?}
[ -f ${real_jar_file?} ] || read -p "ERROR: jar must exist ('${real_jar_file?}')"
if $use_docker; then
 ! [ -h ${real_jar_file?} ] || { # links don't work with docker
	jar_file_name="dcc-etl-docker.jar"
	cp ${real_jar_file?} ${real_etl_dir?}/lib/${jar_file_name?}
  }
fi

release_number=18
patch_number=0
dictionary_version="0.10a"
release_name="${release_prefix?}${release_number?}"
default_parent_dir="/icgc/submission/ICGC${release_number?}"
overarch_command="${overarch_dir?}/overarch.sh ${etl_dir?} ${release_prefix?} ${release_number?} ${patch_number?} ${default_parent_dir?} ${etl_dir?}/conf/projects/${projects_file_name?} ${etl_dir?}/lib/${jar_file_name?} ${etl_dir?}/conf/etl_${cluster?}.yaml ${etl_dir?}/conf/dictionaries/${dictionary_version?}.json ${etl_dir?}/conf/codelists.json ${components_to_run?}"
echo $overarch_command

# ===========================================================================

cd ${git_dir?}
pwd
if $use_docker; then
 docker_command="dcc-etl-cli/src/main/scripts/overarch/helpers/cdh-docker.sh \"${git_dir?}\" \"${overarch_command?}\""
 echo ${docker_command?}
 eval ${docker_command?}
else
 eval "${overarch_command?}"
fi

# ===========================================================================
