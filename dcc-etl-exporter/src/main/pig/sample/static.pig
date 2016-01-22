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

%default DATATYPE 'sample'
%default STATIC_FILE_NAME_PREFIX '<from-param>'

set job.name static-$DATATYPE;

%default RELEASE_OUT 'r12';
%default TMP_STATIC_DIR    '<tmp_static_dir>'
%default OUT_STATIC_DIR    '<tmp_dynamic_dir>'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_SPECIMEN ['_specimen_id'#'$EMPTY_VALUE','specimen_id'#'$EMPTY_VALUE']
%declare EMPTY_SAMPLE ['_sample_id'#'$EMPTY_VALUE','analyzed_sample_id'#'$EMPTY_VALUE','analyzed_sample_interval'#'$EMPTY_VALUE']

-- load donor 
import 'projection.pig';

specimens = FOREACH selected_donor 
          GENERATE icgc_donor_id..submitted_donor_id,
                   FLATTEN(((specimens is null or IsEmpty(specimens)) ? {($EMPTY_SPECIMEN)} : specimens)) as specimen;

content = FOREACH specimens 
          GENERATE icgc_donor_id..submitted_donor_id,
                   specimen#'_specimen_id' as icgc_specimen_id, 
                   specimen#'specimen_id' as submitted_specimen_id, 
                   FLATTEN((bag{tuple(map[])}) specimen#'sample') as s;

static_out = FOREACH content 
             GENERATE s#'_sample_id' as icgc_sample_id,
                      project_code,
                      s#'analyzed_sample_id' as submitted_sample_id,
                      icgc_specimen_id, 
                      submitted_specimen_id,
                      icgc_donor_id,
                      submitted_donor_id,
                      s#'analyzed_sample_interval' as analyzed_sample_interval,
                      s#'percentage_cellularity' as percentage_cellularity,
                      s#'level_of_cellularity' as level_of_cellularity,
                      s#'study' as study;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage('$TMP_STATIC_DIR', '$STATIC_FILE_NAME_PREFIX', 'project_code', 'gz', '\\t');
