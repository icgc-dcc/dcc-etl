%default OBSERVATION '<observation_dir>';
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();
%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader'

-- load donor 
donor = LOAD '$OBSERVATION' USING $JSON_LOADER('-nestedLoad') as document:map[];

selected_donor = foreach donor generate ExtractId(document#'_donor_id') as donor_id:int,
					                    document#'_donor_id' as icgc_donor_id,
                                        document#'_project_id' as project_code,
                                        document#'study_donor_involved_in' as study_donor_involved_in,
                                        document#'donor_id' as submitted_donor_id,
                                        document#'donor_sex' as donor_sex,
                                        document#'donor_vital_status' as donor_vital_status,
                                        document#'disease_status_last_followup' as disease_status_last_followup,
                                        document#'donor_relapse_type' as donor_relapse_type,
                                        document#'donor_age_at_diagnosis' as donor_age_at_diagnosis,
                                        document#'donor_age_at_enrollment' as donor_age_at_enrollment,
                                        document#'donor_age_at_last_followup' as donor_age_at_last_followup,
                                        document#'donor_relapse_interval' as donor_relapse_interval,
                                        document#'donor_diagnosis_icd10' as donor_diagnosis_icd10,
                                        document#'donor_tumour_staging_system_at_diagnosis' as donor_tumour_staging_system_at_diagnosis,
                                        document#'donor_tumour_stage_at_diagnosis' as donor_tumour_stage_at_diagnosis,
                                        document#'donor_tumour_stage_at_diagnosis_supplemental' as donor_tumour_stage_at_diagnosis_supplemental,
                                        document#'donor_survival_time' as donor_survival_time,
                                        document#'donor_interval_of_last_followup' as donor_interval_of_last_followup,
                                        document#'prior_malignancy' as prior_malignancy,
                                        document#'cancer_type_prior_malignancy' as cancer_type_prior_malignancy,
                                        document#'cancer_history_first_degree_relative' as cancer_history_first_degree_relative;