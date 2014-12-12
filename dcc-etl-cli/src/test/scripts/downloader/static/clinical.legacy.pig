-- ===========================================================================
/*
 Do not use directly, only call this from another pig script (see such other script for example of call)
 */

-- Load clinical files
donor =
  LOAD '/icgc/normalizer/$run_name/$project_id/$donor_file'
  AS (donor_id: chararray, donor_sex: chararray, donor_region_of_residence: chararray, donor_vital_status: chararray, disease_status_last_followup: chararray, donor_relapse_type: chararray, donor_age_at_diagnosis: chararray, donor_age_at_enrollment: chararray, donor_age_at_last_followup: chararray, donor_relapse_interval: chararray, donor_diagnosis_icd10: chararray, donor_tumour_staging_system_at_diagnosis: chararray, donor_tumour_stage_at_diagnosis: chararray, donor_tumour_stage_at_diagnosis_supplemental: chararray, donor_survival_time: chararray, donor_interval_of_last_followup: chararray, uri: chararray, db_xref: chararray, donor_notes: chararray);
specimen =
  LOAD '/icgc/normalizer/$run_name/$project_id/$specimen_file'
  AS (donor_id: chararray, specimen_id: chararray, specimen_type: chararray, specimen_type_other: chararray, specimen_interval: chararray, specimen_donor_treatment_type: chararray, specimen_donor_treatment_type_other: chararray, specimen_processing: chararray, specimen_processing_other: chararray, specimen_storage: chararray, specimen_storage_other: chararray, tumour_confirmed: chararray, specimen_biobank: chararray, specimen_biobank_id: chararray, specimen_available: chararray, tumour_histological_type: chararray, tumour_grading_system: chararray, tumour_grade: chararray, tumour_grade_supplemental: chararray, tumour_stage_system: chararray, tumour_stage: chararray, tumour_stage_supplemental: chararray, digital_image_of_stained_section: chararray, uri: chararray, db_xref: chararray, specimen_notes: chararray);
sample2 = -- it appears "sample" is reserved
  LOAD '/icgc/normalizer/$run_name/$project_id/$sample_file'
  AS (analyzed_sample_id: chararray, specimen_id: chararray, analyzed_sample_type: chararray, analyzed_sample_type_other: chararray, analyzed_sample_interval: chararray, uri: chararray, db_xref: chararray, analyzed_sample_notes: chararray);


-- ---------------------------------------------------------------------------
-- Filter out headers (using PK field names for now)
donor =
  FILTER donor
  BY donor_id != 'donor_id';
specimen =
  FILTER specimen
  BY specimen_id != 'specimen_id';
sample2 =
  FILTER sample2
  BY analyzed_sample_id != 'analyzed_sample_id';


-- ---------------------------------------------------------------------------
-- Project fields of interest
donor =
  FOREACH donor
  GENERATE TRIM(donor_id) AS donor_id;

specimen =
  FOREACH specimen
  GENERATE TRIM(donor_id) AS donor_id, TRIM(specimen_id) AS specimen_id;

sample2 =
  FOREACH sample2
  GENERATE TRIM(specimen_id) AS specimen_id, TRIM(analyzed_sample_id) AS analyzed_sample_id;


-- ---------------------------------------------------------------------------
-- Join files
donorXspecimen =
  JOIN
    specimen BY donor_id,
    donor BY donor_id
  USING 'replicated';

donorXspecimenXsample =
  JOIN
    sample2 BY specimen_id,
    donorXspecimen BY specimen_id
  USING 'replicated';

donorXsample =
 FOREACH donorXspecimenXsample
 GENERATE donor::donor_id, sample2::analyzed_sample_id;

-- ===========================================================================

