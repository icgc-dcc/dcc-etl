%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'specimen'

%default RELEASE_OUT '<release>';

%default TMP_STATIC_DIR    '<static_dir>'
%default OUT_STATIC_DIR    '<dynamic_dir>'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_SPECIMEN ['_specimen_id'#'$EMPTY_VALUE','specimen_id'#'$EMPTY_VALUE','study_specimen_involved_in'#'$EMPTY_VALUE','specimen_type'#'$EMPTY_VALUE','specimen_type_other'#'$EMPTY_VALUE','specimen_interval'#'$EMPTY_VALUE','specimen_donor_treatment_type'#'$EMPTY_VALUE','specimen_donor_treatment_type_other'#'$EMPTY_VALUE','specimen_processing'#'$EMPTY_VALUE','specimen_processing_other'#'$EMPTY_VALUE','specimen_storage'#'$EMPTY_VALUE','specimen_storage_other'#'$EMPTY_VALUE','tumour_confirmed'#'$EMPTY_VALUE','specimen_biobank'#'$EMPTY_VALUE','specimen_biobank_id'#'$EMPTY_VALUE','specimen_available'#'$EMPTY_VALUE','tumour_histological_type'#'$EMPTY_VALUE','tumour_grading_system'#'$EMPTY_VALUE','tumour_grade'#'$EMPTY_VALUE','tumour_grade_supplemental'#'$EMPTY_VALUE','tumour_stage_system'#'$EMPTY_VALUE','tumour_stage'#'$EMPTY_VALUE','tumour_stage_supplemental'#'$EMPTY_VALUE','digital_image_of_stained_section'#'$EMPTY_VALUE']

set job.name static-$DATATYPE;
import 'projection.pig';


content = FOREACH selected_donor 
             GENERATE icgc_donor_id..submitted_donor_id, 
             FLATTEN(((specimens is null or IsEmpty(specimens)) ? {($EMPTY_SPECIMEN)} : specimens)) as specimen;

static_out = FOREACH content GENERATE specimen#'_specimen_id' as icgc_specimen_id,
                                      project_code, 
                                      specimen#'study_specimen_involved_in' as study_specimen_involved_in, 
                                      specimen#'specimen_id' as submitted_specimen_id,
                                      icgc_donor_id,
                                      submitted_donor_id, 
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

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage('$TMP_STATIC_DIR', 'specimen', 'project_code', 'gz', '\\t');
