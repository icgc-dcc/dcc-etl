%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'donor_exposure'
-- import

%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '<dynamic_dir>'
%default TMP_HFILE_DIR     '<hfile_dir>'
%default COMPRESSION_ENABLED 'true'

DEFINE COMPUTE_SIZE org.icgc.dcc.etl.exporter.pig.udf.ComputeSize();
DEFINE COMPOSITE_KEY org.icgc.dcc.etl.exporter.pig.udf.CompositeKey('$DATATYPE', '$UPLOAD_TO_RELEASE', '500000');
DEFINE DYNAMIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.DynamicStorage('$DATATYPE', '$UPLOAD_TO_RELEASE', '$COMPRESSION_ENABLED');
DEFINE STATISTIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.StatisticStorage('$DATATYPE', '$UPLOAD_TO_RELEASE');

%default EMPTY_VALUE '';
%declare EMPTY_EXPOSURE ['exposure_type'#'$EMPTY_VALUE','exposure_intensity'#'$EMPTY_VALUE','tobacco_smoking_history_indicator'#'$EMPTY_VALUE','tobacco_smoking_intensity'#'$EMPTY_VALUE','alcohol_history'#'$EMPTY_VALUE','alcohol_history_intensity'#'$EMPTY_VALUE']

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

set job.name dynamic-$DATATYPE;
-- load donor
import 'projection.pig';

flat_donor = FOREACH selected_donor GENERATE donor_id,
                                             icgc_donor_id..submitted_donor_id,
                                             FLATTEN(exposures) as exposure;

selected_content = FOREACH flat_donor GENERATE donor_id,
                                               COMPOSITE_KEY(donor_id) as rowkey:bytearray,
                                               icgc_donor_id..submitted_donor_id,
                                               exposure#'exposure_type' as exposure_type,
                                               exposure#'exposure_intensity' as exposure_intensity,
                                               exposure#'tobacco_smoking_history_indicator' as tobacco_smoking_history_indicator,
                                               exposure#'tobacco_smoking_intensity' as tobacco_smoking_intensity,
                                               exposure#'alcohol_history' as alcohol_history,
                                               exposure#'alcohol_history_intensity' as alcohol_history_intensity;

obj = FOREACH selected_content GENERATE rowkey, {(icgc_donor_id..alcohol_history_intensity)};
content = ORDER obj BY rowkey ASC PARALLEL $DEFAULT_PARALLEL;
STORE content INTO '$TMP_HFILE_DIR' USING DYNAMIC_STORAGE();

-- populate statistics
row_size = FOREACH selected_content GENERATE donor_id, COMPUTE_SIZE(icgc_donor_id..alcohol_history_intensity) as bytes:long;
row_size_per_donor = GROUP row_size BY donor_id;
stats = FOREACH row_size_per_donor GENERATE group as donor_id, COUNT(row_size.donor_id) as total_line, SUM(row_size.bytes) as total_size;
STORE stats INTO '$TMP_DYNAMIC_DIR' USING STATISTIC_STORAGE();