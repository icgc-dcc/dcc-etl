%default LIB 'udf/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
REGISTER $LIB

%default DATATYPE 'specimen'
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

content = FOREACH selected_donor
          GENERATE donor_id, icgc_donor_id..submitted_donor_id,
          FLATTEN(((specimens is null or IsEmpty(specimens)) ? {($EMPTY_SPECIMEN)} : specimens)) as specimen;

specimen_content = FOREACH content GENERATE donor_id, specimen#'_specimen_id' as icgc_specimen_id,
                           project_code,
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

flat_sample = FOREACH content GENERATE specimen#'_specimen_id' as icgc_specimen_id,
                                       FLATTEN((bag{tuple(map[])}) specimen#'sample') as s;

flat_study = FOREACH flat_sample GENERATE icgc_specimen_id,
                                          s#'study' as study;

filter_study = FILTER flat_study by study is not null;

study_field  = FOREACH (GROUP filter_study BY icgc_specimen_id) GENERATE FLATTEN(filter_study);

specimen_with_study = JOIN specimen_content by icgc_specimen_id LEFT OUTER, study_field BY icgc_specimen_id;

selected_specimen = FOREACH specimen_with_study GENERATE
                                      specimen_content::donor_id as donor_id,
                                      specimen_content::icgc_specimen_id as icgc_specimen_id,
                                      specimen_content::project_code as project_code,
                                      study_field::filter_study::study as study_specimen_involved_in,
                                      specimen_content::submitted_specimen_id as submitted_specimen_id,
                                      specimen_content::icgc_donor_id as icgc_donor_id,
                                      specimen_content::submitted_donor_id as submitted_donor_id,
                                      specimen_content::specimen_type as specimen_type,
                                      specimen_content::specimen_type_other as specimen_type_other,
                                      specimen_content::specimen_interval as specimen_interval,
                                      specimen_content::specimen_donor_treatment_type as specimen_donor_treatment_type,
                                      specimen_content::specimen_donor_treatment_type_other as specimen_donor_treatment_type_other,
                                      specimen_content::specimen_processing as specimen_processing,
                                      specimen_content::specimen_processing_other as specimen_processing_other,
                                      specimen_content::specimen_storage as specimen_storage,
                                      specimen_content::specimen_storage_other as specimen_storage_other,
                                      specimen_content::tumour_confirmed as tumour_confirmed,
                                      specimen_content::specimen_biobank as specimen_biobank,
                                      specimen_content::specimen_biobank_id as specimen_biobank_id,
                                      specimen_content::specimen_available as specimen_available,
                                      specimen_content::tumour_histological_type as tumour_histological_type,
                                      specimen_content::tumour_grading_system as tumour_grading_system,
                                      specimen_content::tumour_grade as tumour_grade,
                                      specimen_content::tumour_grade_supplemental as tumour_grade_supplemental,
                                      specimen_content::tumour_stage_system as tumour_stage_system,
                                      specimen_content::tumour_stage as tumour_stage,
                                      specimen_content::tumour_stage_supplemental as tumour_stage_supplemental,
                                      specimen_content::digital_image_of_stained_section as digital_image_of_stained_section,
                                      specimen_content::percentage_cellularity as percentage_cellularity,
                                      specimen_content::level_of_cellularity as level_of_cellularity;



keys = foreach (GROUP selected_specimen BY donor_id) {
             selection = FOREACH selected_specimen GENERATE icgc_specimen_id..level_of_cellularity;
             GENERATE FLATTEN(TOHFILE(group, selection)) as key;
}

STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();