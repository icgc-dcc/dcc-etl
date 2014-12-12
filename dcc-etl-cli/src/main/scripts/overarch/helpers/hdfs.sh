#!/bin/bash -e
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

