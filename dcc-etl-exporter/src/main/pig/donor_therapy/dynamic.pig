%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'donor_therapy'
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
%declare EMPTY_THERAPY ['first_therapy_type'#'$EMPTY_VALUE','first_therapy_therapeutic_intent'#'$EMPTY_VALUE','first_therapy_start_interval'#'$EMPTY_VALUE','first_therapy_duration'#'$EMPTY_VALUE','first_therapy_response'#'$EMPTY_VALUE','second_therapy_type'#'$EMPTY_VALUE','second_therapy_therapeutic_intent'#'$EMPTY_VALUE','second_therapy_start_interval'#'$EMPTY_VALUE','second_therapy_duration'#'$EMPTY_VALUE','second_therapy_response'#'$EMPTY_VALUE','other_therapy'#'$EMPTY_VALUE','other_therapy_response'#'$EMPTY_VALUE']

set job.name dynamic-$DATATYPE;
-- load donor
import 'projection.pig';

flat_donor = FOREACH selected_donor GENERATE donor_id,
                                             icgc_donor_id..submitted_donor_id,
                                             FLATTEN(therapies) as therapy;

selected_content = FOREACH flat_donor GENERATE donor_id,
                                               COMPOSITE_KEY(donor_id) as rowkey:bytearray,
                                               icgc_donor_id..submitted_donor_id,
                                               therapy#'first_therapy_type' as first_therapy_type,
                                               therapy#'first_therapy_therapeutic_intent' as first_therapy_therapeutic_intent,
                                               therapy#'first_therapy_start_interval' as first_therapy_start_interval,
                                               therapy#'first_therapy_duration' as first_therapy_duration,
                                               therapy#'first_therapy_response' as first_therapy_response,
                                               therapy#'second_therapy_type' as second_therapy_type,
                                               therapy#'second_therapy_therapeutic_intent' as second_therapy_therapeutic_intent,
                                               therapy#'second_therapy_start_interval' as second_therapy_start_interval,
                                               therapy#'second_therapy_duration' as second_therapy_duration,
                                               therapy#'second_therapy_response' as second_therapy_response,
                                               therapy#'other_therapy' as other_therapy,
                                               therapy#'other_therapy_response' as other_therapy_response;

obj = FOREACH selected_content GENERATE rowkey, {(icgc_donor_id..other_therapy_response)};
content = ORDER obj BY rowkey ASC PARALLEL $DEFAULT_PARALLEL;
STORE content INTO '$TMP_HFILE_DIR' USING DYNAMIC_STORAGE();

-- populate statistics
row_size = FOREACH selected_content GENERATE donor_id, COMPUTE_SIZE(icgc_donor_id..other_therapy_response) as bytes:long;
row_size_per_donor = GROUP row_size BY donor_id;
stats = FOREACH row_size_per_donor GENERATE group as donor_id, COUNT(row_size.donor_id) as total_line, SUM(row_size.bytes) as total_size;
STORE stats INTO '$TMP_DYNAMIC_DIR' USING STATISTIC_STORAGE();