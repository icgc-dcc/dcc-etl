%default OBSERVATION '<observation_dir>';
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();
%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader'
%default EMPTY_VALUE '';
%declare EMPTY_SPECIMEN ['_specimen_id'#'$EMPTY_VALUE','specimen_id'#'$EMPTY_VALUE','specimen_type'#'$EMPTY_VALUE','specimen_type_other'#'$EMPTY_VALUE','specimen_interval'#'$EMPTY_VALUE','specimen_donor_treatment_type'#'$EMPTY_VALUE','specimen_donor_treatment_type_other'#'$EMPTY_VALUE','specimen_processing'#'$EMPTY_VALUE','specimen_processing_other'#'$EMPTY_VALUE','specimen_storage'#'$EMPTY_VALUE','specimen_storage_other'#'$EMPTY_VALUE','tumour_confirmed'#'$EMPTY_VALUE','specimen_biobank'#'$EMPTY_VALUE','specimen_biobank_id'#'$EMPTY_VALUE','specimen_available'#'$EMPTY_VALUE','tumour_histological_type'#'$EMPTY_VALUE','tumour_grading_system'#'$EMPTY_VALUE','tumour_grade'#'$EMPTY_VALUE','tumour_grade_supplemental'#'$EMPTY_VALUE','tumour_stage_system'#'$EMPTY_VALUE','tumour_stage'#'$EMPTY_VALUE','tumour_stage_supplemental'#'$EMPTY_VALUE','digital_image_of_stained_section'#'$EMPTY_VALUE']

-- load donor 
donor = LOAD '$OBSERVATION' USING $JSON_LOADER('-nestedLoad') as document:map[];

selected_donor = foreach donor generate ExtractId(document#'_donor_id') as donor_id:int,
					                    document#'_donor_id' as icgc_donor_id,
                                        document#'_project_id' as project_code,
                                        document#'donor_id' as submitted_donor_id,
                    (bag{tuple(map[])}) document#'specimen' as specimens;