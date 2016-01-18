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


run_name=${1?} && shift # load-prod-06e-32-22, load-prod-06e-40-23

run_name=load-prod-06e-41-26
run_number=11
type=stsm

mkdir /nfs/dcc_public/dcc/data/testing/${run_name?}
mkdir /nfs/dcc_public/dcc/data/testing/${run_name?}/${type?}

dir=/hdfs/dcc/icgc/testing/${run_name?}-${run_number?}
for project_id in $(ls -d ${dir?}/*.${type?} | awk -F$'/' '{print $NF}' | awk -F$'.' '{print $1}' | sort -u); do
 subdir=${dir?}/${project_id?}.${type?}
 {
  cat ${subdir?}/.pig_header | tr '\t' '\n' | awk -F$':' '{print $NF}' | tr '\n' '\t' | sed 's/\t$//' # get rid of qualifiers (qualifier1::qualifier2::field)
  echo
  cat ${subdir?}/part-*
 } > /nfs/dcc_public/dcc/data/testing/${run_name?}/${type?}/${project_id?}_${type?}.tsv
done


