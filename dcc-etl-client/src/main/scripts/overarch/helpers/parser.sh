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


