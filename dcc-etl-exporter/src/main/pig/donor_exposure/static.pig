%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'donor_exposure'
%default STATIC_FILE_NAME_PREFIX '<from-param>'

%default RELEASE_OUT '<release>';

%default TMP_STATIC_DIR    '<static_dir>'
%default OUT_STATIC_DIR    '<dynamic_dir>'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

set job.name static-$DATATYPE;
import 'projection.pig';


content = FOREACH selected_donor 
             GENERATE icgc_donor_id..submitted_donor_id, 
             FLATTEN(((exposures is null or IsEmpty(exposures)) ? {($EMPTY_EXPOSURE)} : exposures)) as exposure;

selected_content = FOREACH content GENERATE icgc_donor_id..submitted_donor_id, 
                                      exposure#'exposure_type' as exposure_type,
                                      exposure#'exposure_intensity' as exposure_intensity,
                                      exposure#'tobacco_smoking_history_indicator' as tobacco_smoking_history_indicator,
                                      exposure#'tobacco_smoking_intensity' as tobacco_smoking_intensity,
                                      exposure#'alcohol_history' as alcohol_history,
                                      exposure#'alcohol_history_intensity' as alcohol_history_intensity;

static_out = ORDER selected_content BY icgc_donor_id;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage('$TMP_STATIC_DIR', '$STATIC_FILE_NAME_PREFIX', 'project_code', 'gz', '\\t');
