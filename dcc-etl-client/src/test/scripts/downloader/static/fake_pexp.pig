-- pexp:
--
-- pexp_m	[analysis_id, analyzed_sample_id, db_xref, experimental_protocol, note, platform, raw_data_accession, raw_data_repository, uri]
-- pexp_p	[analysis_id, analyzed_sample_id, antibody_id, db_xref, gene_build_version, gene_name, gene_stable_id, normalized_expression_level, note, uri, verification_platform, verification_status]
--
-- pexp_m -> sample [fontsize=8, label="analyzed_sample_id"]
-- pexp_p -> pexp_m [fontsize=8, label="analysis_id,analyzed_sample_id\n(surjective)"]
--
-- 	0	icgc_donor_id
-- 	1	project_code
-- 	2	icgc_specimen_id
-- 	3	icgc_sample_id
-- 	4	submitted_sample_id
-- 	5	analysis_id
-- 	6	analyzed_sample_id
-- 	7	antibody_id
-- 	8	gene_name
-- 	9	gene_stable_id
-- 	10	gene_build_version
-- 	11	normalized_expression_level
-- 	12	verification_status
-- 	13	verification_platform
-- 	14	analysis_id
-- 	15	analyzed_sample_id
-- 	16	platform
-- 	17	experimental_protocol
-- 	18	raw_data_repository
-- 	19	raw_data_accession
--
REGISTER 'udfs.py' using jython as udfs;
DEFINE CLEAR udfs.clear;
set default_parallel $default_parallel
pexp_m = LOAD '$m_file' AS ($m_fields);
pexp_p = LOAD '$p_file' AS ($p_fields);
pexp_m = FILTER pexp_m BY analysis_id != 'analysis_id';
pexp_p = FILTER pexp_p BY analysis_id != 'analysis_id';
pexp_m = FOREACH pexp_m GENERATE $m_normal;
pexp_p = FOREACH pexp_p GENERATE $p_normal;
run -param etl_run_name=$etl_run_name -param project_id=$project_id -param donor_file=$donor_file -param specimen_file=$specimen_file -param sample_file=$sample_file clinical.pig
pexp_m = JOIN
    pexp_m BY analyzed_sample_id,
    clinical BY analyzed_sample_id
  USING 'replicated';
pexp_mXpXs = JOIN
    pexp_p BY (analysis_id, analyzed_sample_id),
    pexp_m BY (analysis_id, pexp_m::analyzed_sample_id)
  USING 'replicated';
pexp =
  FOREACH pexp_mXpXs
  GENERATE
    icgc_donor_id,
    project_code,
    pexp_m::analyzed_sample_id AS submitted_sample_id,
    pexp_m::analysis_id,
    pexp_p::gene_stable_id;
pexp = DISTINCT pexp;
pexp = ORDER pexp BY *;
STORE pexp INTO '$output_dir' USING PigStorage('\t','-schema');

