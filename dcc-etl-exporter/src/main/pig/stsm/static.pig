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

%default DATATYPE 'stsm'
%default TMP_STATIC_DIR    '<tmp_static_dir>';

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['gene_affected_by_bkpt_from'#'$EMPTY_VALUE','gene_affected_by_bkpt_to'#'$EMPTY_VALUE','transcript_affected_by_bkpt_from'#'$EMPTY_VALUE','transcript_affected_by_bkpt_to'#'$EMPTY_VALUE','bkpt_from_context'#'$EMPTY_VALUE','bkpt_to_context'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

set job.name static-$DATATYPE;

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';
content = FOREACH selected_stsm 
          GENERATE icgc_donor_id..verification_platform,
                   FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence, 
                   platform..raw_data_accession;
            
static_out = FOREACH content 
             GENERATE icgc_donor_id..verification_platform,
                      consequence#'gene_affected_by_bkpt_from' as gene_affected_by_bkpt_from,
                      consequence#'gene_affected_by_bkpt_to' as gene_affected_by_bkpt_to,
                      consequence#'transcript_affected_by_bkpt_from' as transcript_affected_by_bkpt_from,
                      consequence#'transcript_affected_by_bkpt_to' as transcript_affected_by_bkpt_to,
                      consequence#'bkpt_from_context' as bkpt_from_context,
                      consequence#'bkpt_to_context' as bkpt_to_context,
                      consequence#'gene_build_version' as gene_build_version,
                      platform..raw_data_accession;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage('$TMP_STATIC_DIR', 'structural_somatic_mutation', 'project_code', 'gz', '\\t');
