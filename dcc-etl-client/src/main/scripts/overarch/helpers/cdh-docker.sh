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
#
# Description:
#   Bind-mounts a specified Docker host directory and runs a command inside a CDH 4.1.2 environment.
#   The docker image `icgcdcc/cdh` is assumed to exist on the host as defined in 
#   https://github.com/icgc-dcc/dcc-docker/blob/master/cdh/Dockerfile. The $CWD will be set to <host_directory>
#   and HDFS FUSE mounted at `/hdfs` before executing the <command>.
#
# Usage:
#   ./cdh-docker.sh <host_directory> <command>
#
# Example:
#   ./cdh-docker.sh ~/docker/app/ "ls /hdfs"

# Prolog
set -o nounset
set -o errexit

# Configuration
readonly base=$1
readonly execute=$2
readonly image=icgcdcc/cdh
readonly mount=$base:/app
readonly command="hadoop-fuse-dfs ro dfs://<host>:8020 /hdfs/dcc &> /dev/null ; cd /app ; $execute" # TODO: get NN from config
readonly etl_dir=***REMOVED***/dcc-etl

# Pull/ Update Docker image
docker pull $image

# Execute $command in the Docker image
docker run \
 --privileged=true -ti --rm \
 \
 -v $mount \
 -v $etl_dir:/etl \
 -v /tmp:/tmp \
 \
 -v $HOME/dcc-exporter/:/dcc-exporter \
 -v $HOME/dcc-exporter/hadoop/conf:/etc/hadoop/conf \
 -v $HOME/dcc-exporter/hbase/conf:/etc/hbase/conf \
 \
 -v /nfs/hadoop/workspace/backups/dcc-etl/dcc-etl-indexer/:/nfs/hadoop/workspace/backups/dcc-etl/dcc-etl-indexer/ \
 \
 -e SUDO_USER=${SUDO_USER:="UNKNOWN"} \
 -e USER=$USER \
 -e HADOOP_USER_NAME="hdfs" \
 -e etl_dir=$etl_dir \
 \
 $image \
 /bin/bash -c \
 "$command"

