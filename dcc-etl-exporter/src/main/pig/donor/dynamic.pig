%default LIB 'udf/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
REGISTER $LIB

%default DATATYPE 'donor'
-- import

%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '<dynamic_dir>'
%default TMP_HFILE_DIR     '<hfile_dir>'
DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_SPECIMEN ['_specimen_id'#'$EMPTY_VALUE','specimen_id'#'$EMPTY_VALUE','specimen_type'#'$EMPTY_VALUE','specimen_type_other'#'$EMPTY_VALUE','specimen_interval'#'$EMPTY_VALUE','specimen_donor_treatment_type'#'$EMPTY_VALUE','specimen_donor_treatment_type_other'#'$EMPTY_VALUE','specimen_processing'#'$EMPTY_VALUE','specimen_processing_other'#'$EMPTY_VALUE','specimen_storage'#'$EMPTY_VALUE','specimen_storage_other'#'$EMPTY_VALUE','tumour_confirmed'#'$EMPTY_VALUE','specimen_biobank'#'$EMPTY_VALUE','specimen_biobank_id'#'$EMPTY_VALUE','specimen_available'#'$EMPTY_VALUE','tumour_histological_type'#'$EMPTY_VALUE','tumour_grading_system'#'$EMPTY_VALUE','tumour_grade'#'$EMPTY_VALUE','tumour_grade_supplemental'#'$EMPTY_VALUE','tumour_stage_system'#'$EMPTY_VALUE','tumour_stage'#'$EMPTY_VALUE','tumour_stage_supplemental'#'$EMPTY_VALUE','digital_image_of_stained_section'#'$EMPTY_VALUE']

set job.name dynamic-$DATATYPE;
-- load donor 
import 'projection.pig';

keys = foreach (GROUP selected_donor BY donor_id) {
	                   selected_content = FOREACH selected_donor GENERATE icgc_donor_id..cancer_history_first_degree_relative;
             GENERATE FLATTEN(TOHFILE(group, selected_content)) as key;
}

STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();