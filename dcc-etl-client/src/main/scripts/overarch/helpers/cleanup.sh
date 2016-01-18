#!/bin/bash -e
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

# Helps cleaning up leftovers from ETL temporary runs
# usage: ./cleanup.sh mode intermediate_dir homogenized_hdfs_dir mongo_server es_server
# example: args="/nfs/dcc_secure/dcc/etl/intermediate /nfs/backups/elasticsearch /icgc/etl /tmp/download/static ***REMOVED*** dcc-etl $PASSWD ***REMOVED***" && ./cleanup.sh list ${args?} | ./cleanup.sh translate ${args?}
# TODO: add hbase equivalent for dynamic download file (Jerry says it's not easy)

# ===========================================================================

mode=${1?} && shift # "list" or "delete"
intermediate_dir=${1?} && shift
vcf_dir=${1?} && shift
homogenized_hdfs_dir=${1?} && shift
static_hdfs_dir=${1?} && shift
mongo_server=${1?} && shift # may include port
mongo_user=${1?} && shift
mongo_passwd=${1?} && shift
es_server=${1?} && shift # may include port

# ===========================================================================
if [[ ! "${mongo_server?}" =~ :[0-9+] ]]; then
 mongo_server="${mongo_server?}:27017"
fi
if [[ ! "${es_server?}" =~ :[0-9+] ]]; then
 es_server="${es_server?}:9200"
fi

# ===========================================================================
# Constants:

fuse_mount_point="/hdfs/dcc"
homogenized_fuse_dir="${fuse_mount_point?}${homogenized_hdfs_dir?}"
static_fuse_dir="${fuse_mount_point?}${static_hdfs_dir?}"

# ===========================================================================
# Utils

function exclude_protected() {
  grep -vi load-prod-06e-41-33 \
  | grep -vi load-prod-07e-42-icgc15-feb07_2 \
  | grep -vi load-prod-07e-42-icgc15-jan31 \
  | grep -vi 07e-42-icgc15-controlled01 \
  | grep -vi all-7 \
  | grep -vi 4p-2 \
  | grep -vi 4p-3 \
  | grep -vi 4p-4 \
  | grep -vi ICGC16-3 \
  | grep -vi ICGC16-3_bug \
  | grep -vi ICGC16-4 \
  | grep -vi ICGC16-5 \
  | grep -vi ICGC16-6 \
  | grep -vi release_16
}

# ===========================================================================

[ -d ${intermediate_dir?} ] || { echo "ERROR: ${intermediate_dir?} does not exist, has it changed in the overarching script maybe?"; exit 1; }
[ -d ${homogenized_fuse_dir?} ] || { echo "ERROR: ${homogenized_fuse_dir?} does not exist, has it changed in the overarching script maybe?"; exit 1; }

# ===========================================================================

if [ "${mode?}" == "list" ]; then

 echo "# mode=\"${mode?}\""
 echo "# intermediate_dir=\"${intermediate_dir?}\""
 echo "# homogenized_hdfs_dir=\"${homogenized_hdfs_dir?}\""
 echo "# mongo_server=\"${mongo_server?}\""
 echo "# mongo_user=\"${mongo_user?}\""
 echo "# mongo_passwd=\"${mongo_passwd?}\""
 echo "# es_server=\"${es_server?}\""
 echo
 
 location="intermediate1"
 find ${intermediate_dir?} -maxdepth 2 -type d | tail -n+2 | sed 's/'$(perl -e "print quotemeta('${intermediate_dir?}/')")'//g' | grep    '/' | sort | awk -F$'\t' -v LOCATION=${location?} '{print LOCATION "\t" $0}'
 echo

 location="intermediate2"
 find ${intermediate_dir?} -maxdepth 2 -type d | tail -n+2 | sed 's/'$(perl -e "print quotemeta('${intermediate_dir?}/')")'//g' | grep -v '/' | sort | awk -F$'\t' -v LOCATION=${location?} '{print LOCATION "\t" $0}'
 echo

 location="vcf"
 find ${vcf_dir?} -type f -name "ssm.*.open.vcf.gz" | sed 's/'$(perl -e "print quotemeta('${vcf_dir?}/')")'//g' | grep -v '/' | sort | awk -F$'\t' -v LOCATION=${location?} '{print LOCATION "\t" $0}'
 echo

 location="homogenized"
 find ${homogenized_fuse_dir?} -maxdepth 2 -type d | tail -n+2 | sed 's/'$(perl -e "print quotemeta('${homogenized_fuse_dir?}/')")'//g' | awk -F$'/' '{print $1}' | sort -u | awk -F$'\t' -v LOCATION=${location?} '{print LOCATION "\t" $0}'
 echo

 location="static"
 find ${static_fuse_dir?} -maxdepth 1 -type d | tail -n+2 | grep -v "/Projects" | grep -v "/dcc-2192" | sed 's/'$(perl -e "print quotemeta('${static_fuse_dir?}/')")'//g' | awk -F$'/' '{print $1}' | sort -u | awk -F$'\t' -v LOCATION=${location?} '{print LOCATION "\t" $0}'
 echo

 location="mongo"
 mongo ${mongo_server?}/admin -u${mongo_user?} -p${mongo_passwd?} --quiet --eval "printjson(db.adminCommand('listDatabases'))" \
  | python -c 'import json,sys; keys = json.loads(sys.stdin.read())["databases"]; print "\n".join([str(x["name"]) for x in keys]);' \
  | awk '!/^dcc-project$/ && !/^local$/ && !/^dcc-genome$/ && !/^admin$/ && !/^seq-meta$/ && !/^test$/ && !/^icgc/' \
  | exclude_protected \
  | sort | awk -F$'\t' -v LOCATION=${location?} '{print LOCATION "\t" $0}'
 echo

 location="es"
 curl http://${es_server?}/_stats 2>&- | python -c 'import json,sys; keys = json.loads(sys.stdin.read())["indices"]; print "\n".join([str(x) for x in keys.keys()])' \
  | grep -v test-index \
  | exclude_protected \
  | sort | awk -F$'\t' -v LOCATION=${location?} '{print LOCATION "\t" $0}'
 echo

 exit
fi

# ===========================================================================

if [ "${mode?}" == "translate" ]; then

 tmp_file="/tmp/etl_cleanup.tmp"
 tee > ${tmp_file?}
  
 echo "#!/bin/bash -x"
 echo

 location="intermediate1"
 for item in $(awk -F$'\t' '$1=="'${location?}'"{print $2}' ${tmp_file?} | tr '/' '-' | exclude_protected | tr '-' '/'); do
  command="rm -rf ${intermediate_dir?}/${item?}"
  echo "${command?}"
 done
 echo
 echo "echo"
 
 location="intermediate2"
 for item in $(awk -F$'\t' '$1=="'${location?}'"{print $2}' ${tmp_file?} | tr '/' '-' | exclude_protected | tr '-' '/'); do
  command="rmdir ${intermediate_dir?}/${item?}"
  echo "${command?}"
 done
 echo
 echo "echo"

 location="vcf"
 for item in $(awk -F$'\t' '$1=="'${location?}'"{print $2}' ${tmp_file?} | exclude_protected); do
  command="rm -f ${vcf_dir?}/${item?}"
  echo "${command?}"
 done
 echo
 echo "echo"

 location="homogenized"
 export HADOOP_USER_NAME=hdfs
 for item in $(awk -F$'\t' '$1=="'${location?}'"{print $2}' ${tmp_file?} | exclude_protected); do
  command="hadoop fs -rm -r -skipTrash ${homogenized_hdfs_dir?}/${item?}"
  echo "${command?}"
 done
 echo
 echo "echo"

 location="static"
 export HADOOP_USER_NAME=hdfs
 for item in $(awk -F$'\t' '$1=="'${location?}'"{print $2}' ${tmp_file?} | exclude_protected); do
  command="hadoop fs -rm -r -skipTrash ${static_hdfs_dir?}/${item?}"
  echo "${command?}"
 done
 echo
 echo "echo"
 
 location="mongo"
 export HADOOP_USER_NAME=hdfs
 for item in $(awk -F$'\t' '$1=="'${location?}'"{print $2}' ${tmp_file?} | exclude_protected); do
  command="mongo ${mongo_server?}/${item?} -u${mongo_user?} -p${mongo_passwd?} --quiet --eval \"printjson(db.dropDatabase())\" --authenticationDatabase admin"
  echo "${command?}"
 done
 echo
 echo "echo"
 
 location="es"
 for item in $(awk -F$'\t' '$1=="'${location?}'"{print $2}' ${tmp_file?} | exclude_protected); do
  command="curl -XDELETE http://${es_server?}/${item?}"
  echo "${command?}"
 done
 echo
 echo "echo"

 exit
fi

# ===========================================================================

echo "ERROR: unknown mode ${mode?}"

# ===========================================================================

