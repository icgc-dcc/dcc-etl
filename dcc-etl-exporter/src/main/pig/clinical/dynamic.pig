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

%default LIB 'udf/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
REGISTER $LIB

%default DATATYPE 'clinical'
-- import

%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '<dynamic_dir>'
%default TMP_HFILE_DIR     '<hfile_dir>'
%default COMPRESSION_ENABLED 'true'

DEFINE COMPUTE_SIZE org.icgc.dcc.etl.exporter.pig.udf.ComputeSize();
DEFINE COMPOSITE_KEY org.icgc.dcc.etl.exporter.pig.udf.CompositeKey('$DATATYPE', '$UPLOAD_TO_RELEASE', '500000');
DEFINE DYNAMIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.DynamicStorage('$DATATYPE', '$UPLOAD_TO_RELEASE', '$COMPRESSION_ENABLED');
DEFINE STATISTIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.StatisticStorage('$DATATYPE', '$UPLOAD_TO_RELEASE');

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_SPECIMEN ['_specimen_id'#'$EMPTY_VALUE','specimen_id'#'$EMPTY_VALUE','specimen_type'#'$EMPTY_VALUE','specimen_type_other'#'$EMPTY_VALUE','specimen_interval'#'$EMPTY_VALUE','specimen_donor_treatment_type'#'$EMPTY_VALUE','specimen_donor_treatment_type_other'#'$EMPTY_VALUE','specimen_processing'#'$EMPTY_VALUE','specimen_processing_other'#'$EMPTY_VALUE','specimen_storage'#'$EMPTY_VALUE','specimen_storage_other'#'$EMPTY_VALUE','tumour_confirmed'#'$EMPTY_VALUE','specimen_biobank'#'$EMPTY_VALUE','specimen_biobank_id'#'$EMPTY_VALUE','specimen_available'#'$EMPTY_VALUE','tumour_histological_type'#'$EMPTY_VALUE','tumour_grading_system'#'$EMPTY_VALUE','tumour_grade'#'$EMPTY_VALUE','tumour_grade_supplemental'#'$EMPTY_VALUE','tumour_stage_system'#'$EMPTY_VALUE','tumour_stage'#'$EMPTY_VALUE','tumour_stage_supplemental'#'$EMPTY_VALUE','digital_image_of_stained_section'#'$EMPTY_VALUE']

set job.name dynamic-$DATATYPE;
-- load donor 
import 'projection.pig';

flat_donor = FOREACH selected_donor GENERATE donor_id, icgc_donor_id..donor_interval_of_last_followup,
                                             FLATTEN(((specimens is null or IsEmpty(specimens)) ? {($EMPTY_SPECIMEN)} : specimens)) as specimen;

selected_content = FOREACH flat_donor GENERATE donor_id, COMPOSITE_KEY(donor_id) as rowkey:bytearray, icgc_donor_id..donor_interval_of_last_followup,
                                               specimen#'_specimen_id' as icgc_specimen_id,
                                               specimen#'specimen_id' as submitted_specimen_id,
                                               specimen#'specimen_type' as specimen_type,
                                               specimen#'specimen_type_other' as specimen_type_other,
                                               specimen#'specimen_interval' as specimen_interval,
                                               specimen#'specimen_donor_treatment_type' as specimen_donor_treatment_type,
                                               specimen#'specimen_donor_treatment_type_other' as specimen_donor_treatment_type_other,
                                               specimen#'specimen_processing' as specimen_processing,
                                               specimen#'specimen_processing_other' as specimen_processing_other,
                                               specimen#'specimen_storage' as specimen_storage,
                                               specimen#'specimen_storage_other' as specimen_storage_other,
                                               specimen#'tumour_confirmed' as tumour_confirmed,
                                               specimen#'specimen_biobank' as specimen_biobank,
                                               specimen#'specimen_biobank_id' as specimen_biobank_id,
                                               specimen#'specimen_available' as specimen_available,
                                               specimen#'tumour_histological_type' as tumour_histological_type,
                                               specimen#'tumour_grading_system' as tumour_grading_system,
                                               specimen#'tumour_grade' as tumour_grade,
                                               specimen#'tumour_grade_supplemental' as tumour_grade_supplemental,
                                               specimen#'tumour_stage_system' as tumour_stage_system,
                                               specimen#'tumour_stage' as tumour_stage,
                                               specimen#'tumour_stage_supplemental' as tumour_stage_supplemental,
                                               specimen#'digital_image_of_stained_section' as digital_image_of_stained_section,
                                               specimen#'percentage_cellularity' as percentage_cellularity,
                                               specimen#'level_of_cellularity' as level_of_cellularity;
obj = FOREACH selected_content GENERATE rowkey, {(icgc_donor_id..level_of_cellularity)};
content = ORDER obj BY rowkey ASC PARALLEL $DEFAULT_PARALLEL;
STORE content INTO '$TMP_HFILE_DIR' USING DYNAMIC_STORAGE();

-- populate statistics
row_size = FOREACH selected_content GENERATE donor_id, COMPUTE_SIZE(icgc_donor_id..level_of_cellularity) as bytes:long;
row_size_per_donor = GROUP row_size BY donor_id;
stats = FOREACH row_size_per_donor GENERATE group as donor_id, COUNT(row_size.donor_id) as total_line, SUM(row_size.bytes) as total_size;
STORE stats INTO '$TMP_DYNAMIC_DIR' USING STATISTIC_STORAGE();