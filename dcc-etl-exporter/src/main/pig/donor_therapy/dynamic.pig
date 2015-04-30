%default LIB 'udf/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
REGISTER $LIB

%default DATATYPE 'donor_therapy'
-- import

%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '<dynamic_dir>'
%default TMP_HFILE_DIR     '<hfile_dir>'
DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_THERAPY ['first_therapy_type'#'$EMPTY_VALUE','first_therapy_therapeutic_intent'#'$EMPTY_VALUE','first_therapy_start_interval'#'$EMPTY_VALUE','first_therapy_duration'#'$EMPTY_VALUE','first_therapy_response'#'$EMPTY_VALUE','second_therapy_type'#'$EMPTY_VALUE','second_therapy_therapeutic_intent'#'$EMPTY_VALUE','second_therapy_start_interval'#'$EMPTY_VALUE','second_therapy_duration'#'$EMPTY_VALUE','second_therapy_response'#'$EMPTY_VALUE','other_therapy'#'$EMPTY_VALUE','other_therapy_response'#'$EMPTY_VALUE']

set job.name dynamic-$DATATYPE;
-- load donor 
import 'projection.pig';

keys = foreach (GROUP selected_donor BY donor_id) {
             content = FOREACH selected_donor 
                          GENERATE icgc_donor_id..submitted_donor_id, 
                          FLATTEN(therapies) as therapy;
             
             selected_content = FOREACH content GENERATE icgc_donor_id..submitted_donor_id, 
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

             GENERATE FLATTEN(TOHFILE(group, selected_content)) as key;
}

STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();