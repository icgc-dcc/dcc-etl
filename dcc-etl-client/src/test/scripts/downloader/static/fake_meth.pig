-- meth
--
-- meth_m	[alignment_algorithm, analysis_id, analyzed_sample_id, assembly_version, base_calling_algorithm, db_xref, experimental_protocol, matched_sample_id, note, other_analysis_algorithm, platform, raw_data_accession, raw_data_repository, uri, variation_calling_algorithm]
-- meth_p	[analysis_id, analyzed_sample_id, beta_value, chromosome, chromosome_end, chromosome_start, chromosome_strand, db_xref, methylated_fragment_id, note, percent_methylation, probability, quality_score, uri, verification_platform, verification_status]
-- meth_s	[analysis_id, analyzed_sample_id, gene_affected, gene_build_version, methylated_fragment_id, note]
--
-- meth_m -> sample [fontsize=8, label="analyzed_sample_id"]
-- meth_m -> sample [fontsize=8, label="matched_sample_id -> analyzed_sample_id"]
-- meth_p -> meth_m [fontsize=8, label="analysis_id,analyzed_sample_id\n(surjective)"]
-- meth_s -> meth_p [fontsize=8, label="analysis_id,analyzed_sample_id,methylated_fragment_id"]
--
-- 	0	icgc_donor_id
-- 	1	project_code
-- 	2	icgc_specimen_id
-- 	3	icgc_sample_id
-- 	4	icgc_sample_id
-- 	5	matched_icgc_sample_id
-- 	6	submitted_sample_id
-- 	7	submitted_matched_sample_id
-- 	8	analysis_id
-- 	9	methylated_fragment_id
-- 	10	chromosome
-- 	11	chromosome_start
-- 	12	chromosome_end
-- 	13	chromosome_strand
-- 	14	assembly_version
-- 	15	percent_methylation
-- 	16	beta_value
-- 	17	quality_score
-- 	18	probability
-- 	19	verification_status
-- 	20	verification_platform
-- 	21	gene_affected
-- 	22	gene_build_version
-- 	23	platform
-- 	24	experimental_protocol
-- 	25	base_calling_algorithm
-- 	26	alignment_algorithm
-- 	27	variation_calling_algorithm
-- 	28	other_analysis_algorithm
-- 	29	raw_data_repository
-- 	30	raw_data_accession
--
REGISTER 'udfs.py' using jython as udfs;
DEFINE CLEAR udfs.clear;
set default_parallel $default_parallel
meth_m = LOAD '$m_file' AS ($m_fields);
meth_p = LOAD '$p_file' AS ($p_fields);
meth_s = LOAD '$s_file' AS ($s_fields);
meth_m = FILTER meth_m BY analysis_id != 'analysis_id';
meth_p = FILTER meth_p BY analysis_id != 'analysis_id';
meth_s = FILTER meth_s BY analysis_id != 'analysis_id';
meth_m = FOREACH meth_m GENERATE $m_normal;
meth_p = FOREACH meth_p GENERATE $p_normal;
meth_s = FOREACH meth_s GENERATE $s_normal;
run -param etl_run_name=$etl_run_name -param project_id=$project_id -param donor_file=$donor_file -param specimen_file=$specimen_file -param sample_file=$sample_file clinical.pig
meth_m = JOIN
    meth_m BY analyzed_sample_id,
    clinical BY analyzed_sample_id
  USING 'replicated';
meth_pXs = JOIN
  meth_p BY (analysis_id, analyzed_sample_id, methylated_fragment_id) LEFT OUTER,
  meth_s BY (analysis_id, analyzed_sample_id, methylated_fragment_id);
meth_mXpXs = JOIN
    meth_pXs BY (meth_p::analysis_id, meth_p::analyzed_sample_id),
    meth_m BY (analysis_id, meth_m::analyzed_sample_id)
  USING 'replicated';
meth =
  FOREACH meth_mXpXs
  GENERATE
    icgc_donor_id,
    project_code,
    meth_m::analyzed_sample_id AS submitted_sample_id,
    meth_m::analysis_id,
    meth_p::methylated_fragment_id,
    meth_s::gene_affected;
meth = DISTINCT meth;
meth = ORDER meth BY *;
STORE meth INTO '$output_dir' USING PigStorage('\t','-schema');
