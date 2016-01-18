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


run_number=${1?} && shift
type=${1?} && shift

run_name=load-prod-06e-41

./submission.sh ${run_number?} ${type?}

ssm_projects="$(find /hdfs/dcc/icgc/normalizer/${run_name?} | grep ssm | awk -F$'/' '{print $7}' | sort -u)"
project_ids=$(find /hdfs/dcc/icgc/normalizer/${run_name?}-${run_number?} | awk '/\/'${type?}'/' | awk -F$'/' '{print $(NF-1)}' | sort -u | tr '\n' ' ')

for project_id in ${ssm_projects?}; do
 printf '=%.0s' {1..75} && echo && echo ${project_id?}
 pig -param run_name=${run_name?} -param run_number=${run_number?} -param project_id=${project_id?} normalizer.pig
 echo
done

for project_id in ${ssm_projects?}; do
 echo -n ${project_id?}
 [ -z "$(diff -q <(cat /hdfs/dcc/icgc/testing/diff/${project_id?}.original/part-*) <(cat /hdfs/dcc/icgc/testing/diff/${project_id?}.static/part-*))" ] && echo ": OK" || echo ": KO..."
done

echo OK

