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

# Utility methods for HDFS (to help with readability)

export HADOOP_USER_NAME=hdfs

# Pushes a LOCAL file to an HDFS directory (leaves origin untouched)
function hdfs_push() { # TODO: add sanity checks
  local_input_file=${1?} && shift # Must be a file
  _hdfs_output_dir=${1?} && shift # Must be a directory
  echo "Pushing: '${local_input_file?}' to '${_hdfs_output_dir?}'"
  hadoop fs -put ${local_input_file?} ${_hdfs_output_dir?}
}

# Pops an HDFS file to a LOCAL directory (removes it from origin)
function hdfs_pop() { # TODO: add sanity checks
  hdfs_input_file=${1?} && shift # Must be a file
  local_output_dir=${1?} && shift # Must be a directory
  echo "Popping: '${hdfs_input_file?}' to '${local_output_dir?}'"
  hadoop fs -get ${hdfs_input_file?} ${local_output_dir?}
  hdfs_rm ${hdfs_input_file?}
}

# Removes an HDFS file
function hdfs_rm() { # TODO: add sanity checks
  hdfs_file=${1?} && shift # Must be a file
  echo "Deleting: '${hdfs_file?}'"
  hadoop fs -rm ${hdfs_file?}
}

function hdfs_rmdir() {
  hdfs_dir=${1?} && shift # Must be a dir
  echo "Removing: '${hdfs_dir?}'"
  hadoop fs -rmdir ${hdfs_dir?}
}

function hdfs_mkdirs() { # TODO: make it "if exists"
  hdfs_dir=${1?} && shift # Must be a dir
  echo "Creating: '${hdfs_dir?}'"
  hadoop fs -mkdir ${hdfs_dir?}
}

function hdfs_ls() {
  hdfs_file=${1?} && shift # Must be a file
  echo "Listing: '${hdfs_file?}'"
  hadoop fs -ls ${hdfs_file?}
}

function hdfs_is_dir() {
  hdfs_dir=${1?} && shift
  echo "Is dir: '${hdfs_dir?}'"
  hadoop fs -test -d ${hdfs_dir?} 2>&-
}

