#/bin/bash -e
# Parses submission input file and applies functions to them

#function f1() { wc -l | awk '{print $1}'; }
#function f2() { echo -e "$f\t$[result-1]"; }
function f1() { wc -l | awk '{print $1}'; }
#function f2() { declare -i c=$result; if [ $c -lt 2 ]; then echo $f; fi;  }
function f2() { echo -e "$result\t$f";  }

function get_stream_command() { # usage: stream_command=$(get_stream_command $f)
 file=${1?} && shift
 filename=$(basename ${file?})
 if [[ "${filename?}" =~ \.txt$ ]]; then # is_plain
  echo "cat ${file?}"
 fi
 if [[ "${filename?}" =~ \.gz$ ]]; then # is_gzip
  echo "gzip -cd ${file?}"
 fi
 if [[ "${filename?}" =~ \.bz2$ ]]; then # is_bzip2
  echo "bzip2 -cd ${file?}"
 fi
}

parent_dir=${1?} && shift # fuse

for p in $(ls ${parent_dir?}); do
 for f in $(ls ${parent_dir?}/$p/*.txt*); do
  stream_command=$(get_stream_command $f)
  result=$(eval "${stream_command?}" | f1)
  f2
 done
done


