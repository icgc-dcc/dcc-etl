-- Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
--
-- This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
-- You should have received a copy of the GNU General Public License along with
-- this program. If not, see <http://www.gnu.org/licenses/>.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
-- EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
-- OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
-- SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
-- INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
-- TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
-- OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
-- IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
-- ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

--set mapred.reduce.parallel.copies 2
set io.sort.mb 1024
set io.sort.factor 100
--set mapred.inmem.merge.threshold 0
--set mapred.job.reduce.input.buffer.percent 1.0
set io.file.buffer.size 131072

%default DEFAULT_PARALLEL '12';
set default_parallel $DEFAULT_PARALLEL;

%default DATATYPE 'meth_seq'
set job.name static-$DATATYPE;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['gene_affected'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

%default TMP_STATIC_DIR    '<tmp_static_dir>'

import 'projection.pig';
static_out = FOREACH selected_meth GENERATE icgc_donor_id..raw_data_accession;
STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage('$TMP_STATIC_DIR', 'meth_seq', 'project_code', 'gz', '\\t');
