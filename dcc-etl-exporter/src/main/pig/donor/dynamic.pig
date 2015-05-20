%default LIB 'udf/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
REGISTER $LIB

%default DATATYPE 'donor'
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

selected_content = FOREACH  selected_donor GENERATE donor_id,
                                                    COMPOSITE_KEY(donor_id) as rowkey:bytearray,
                                                    icgc_donor_id..cancer_history_first_degree_relative;

obj = FOREACH selected_content GENERATE rowkey, {(icgc_donor_id..cancer_history_first_degree_relative)};
content = ORDER obj BY rowkey ASC PARALLEL $DEFAULT_PARALLEL;
STORE content INTO '$TMP_HFILE_DIR' USING DYNAMIC_STORAGE();

-- populate statistics
row_size = FOREACH selected_content GENERATE donor_id, COMPUTE_SIZE(icgc_donor_id..cancer_history_first_degree_relative) as bytes:long;
row_size_per_donor = GROUP row_size BY donor_id;
stats = FOREACH row_size_per_donor GENERATE group as donor_id, COUNT(row_size.donor_id) as total_line, SUM(row_size.bytes) as total_size;
STORE stats INTO '$TMP_DYNAMIC_DIR' USING STATISTIC_STORAGE();