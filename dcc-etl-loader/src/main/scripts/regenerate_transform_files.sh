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

# usage: must be in loader dir, must provide path to dictionary file
# example: data-submission$ ./loader/src/main/scripts/regenerate_transform_files.sh $PWD/server/src/main/resources/0.6c.CLOSED.json

[ "$(basename $PWD)" == "data-submission" ] || { echo "ERROR: must be in data-submission dir"; exit 1; }
dictionary_file=${1?}

file_pattern="loader/target/classes/transform/*.gen.transform.json"
rm ${file_pattern?} 2>&- || :

cd loader
mvn exec:java -Dexec.mainClass="org.icgc.dcc.submission.etl.loader.generator.Main" -Dexec.args="${dictionary_file?}"
cd ..

for file in $(ls -1 ${file_pattern?}); do
 filename=$(basename ${file?} | sed 's/\.gen//g')
 loader/src/main/scripts/format.sh ${file?} > loader/src/main/resources/transform/${filename?}
done
echo
git status --porcelain loader/src/main/resources/transform
echo
