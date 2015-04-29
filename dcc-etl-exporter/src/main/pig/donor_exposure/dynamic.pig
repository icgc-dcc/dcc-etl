%default LIB 'udf/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
REGISTER $LIB

%default DATATYPE 'donor_exposure'
-- import

%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '<dynamic_dir>'
%default TMP_HFILE_DIR     '<hfile_dir>'
DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

set job.name dynamic-$DATATYPE;
-- load donor 
import 'projection.pig';

keys = foreach (GROUP selected_donor BY donor_id) {
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
             GENERATE FLATTEN(TOHFILE(group, selected_content)) as key;
}
STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();