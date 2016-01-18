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
#   export data for static download in the portal for a given release
#
# Usage:
#   ./static-export.sh <release_name> <loader_output_directory> <data_types>
#
# Example:
#   ./static-export.sh test_release /icgc/overarch/test18/0/7/loader clinical,clinicalsample,ssm_open

# Prolog
set -o nounset
set -o errexit

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR
source ${EXPORTHOMEDIR}/bin/setenv.sh

start_time=`date +%s`

# Export
${EXPORTHOMEDIR}/bin/parallel -r -j 4 "${EXPORTHOMEDIR}/lib/pig/bin/pig -l ${EXPORTHOMEDIR}/logs/*.log -4 ${EXPORTHOMEDIR}/conf/log4j.properties ${EXPORTHOMEDIR}/python/exporter.py -s -d * -e ${EXPORTHOMEDIR}/pig -i ${source} -r ${release} -l ${logfile}" ${types[@]}

end_time=`date +%s`
echo Total static export time was `expr $end_time - $start_time` s.
