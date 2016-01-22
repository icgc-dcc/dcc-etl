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

%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'stsm'
%default UPLOAD_TO_RELEASE '';
%default DEFAULT_PARALLEL '3';
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'
%default COMPRESSION_ENABLED 'true'

DEFINE COMPUTE_SIZE org.icgc.dcc.etl.exporter.pig.udf.ComputeSize();
DEFINE COMPOSITE_KEY org.icgc.dcc.etl.exporter.pig.udf.CompositeKey('$DATATYPE', '$UPLOAD_TO_RELEASE', '500000');
DEFINE DYNAMIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.DynamicStorage('$DATATYPE', '$UPLOAD_TO_RELEASE', '$COMPRESSION_ENABLED');
DEFINE STATISTIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.StatisticStorage('$DATATYPE', '$UPLOAD_TO_RELEASE');

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['gene_affected_by_bkpt_from'#'$EMPTY_VALUE','gene_affected_by_bkpt_to'#'$EMPTY_VALUE','transcript_affected_by_bkpt_from'#'$EMPTY_VALUE','transcript_affected_by_bkpt_to'#'$EMPTY_VALUE','bkpt_from_context'#'$EMPTY_VALUE','bkpt_to_context'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

set job.name dynamic-$DATATYPE;
set default_parallel $DEFAULT_PARALLEL;

%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'

import 'projection.pig';
-- Dynamic --
flat_stsm = FOREACH selected_stsm GENERATE donor_id,
                                         icgc_donor_id..verification_platform,
                                         FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence,
                                         platform..raw_data_accession;

selected_content = FOREACH flat_stsm GENERATE donor_id,
                                              COMPOSITE_KEY(donor_id) as rowkey:bytearray,
                                              icgc_donor_id..verification_platform,
                                              consequence#'gene_affected_by_bkpt_from' as gene_affected_by_bkpt_from,
                                              consequence#'gene_affected_by_bkpt_to' as gene_affected_by_bkpt_to,
                                              consequence#'transcript_affected_by_bkpt_from' as transcript_affected_by_bkpt_from,
                                              consequence#'transcript_affected_by_bkpt_to' as transcript_affected_by_bkpt_to,
                                              consequence#'bkpt_from_context' as bkpt_from_context,
                                              consequence#'bkpt_to_context' as bkpt_to_context,
                                              consequence#'gene_build_version' as gene_build_version,
                                              platform..raw_data_accession;

obj = FOREACH selected_content GENERATE rowkey, {(icgc_donor_id..raw_data_accession)};
content = ORDER obj BY rowkey ASC PARALLEL $DEFAULT_PARALLEL;
STORE content INTO '$TMP_HFILE_DIR' USING DYNAMIC_STORAGE();

-- populate statistics
row_size = FOREACH selected_content GENERATE donor_id, COMPUTE_SIZE(icgc_donor_id..raw_data_accession) as bytes:long;
row_size_per_donor = GROUP row_size BY donor_id;
stats = FOREACH row_size_per_donor GENERATE group as donor_id, COUNT(row_size.donor_id) as total_line, SUM(row_size.bytes) as total_size;
STORE stats INTO '$TMP_DYNAMIC_DIR' USING STATISTIC_STORAGE();