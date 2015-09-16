-- exp:
--
-- exp_m	[alignment_algorithm, analysis_id, analyzed_sample_id, assembly_version, base_calling_algorithm, db_xref, experimental_protocol, gene_build_version, normalization_algorithm, note, other_analysis_algorithm, platform, raw_data_accession, raw_data_repository, seq_coverage, sequencing_strategy, uri]
-- exp_g	[analysis_id, analyzed_sample_id, db_xref, fold_change, gene_chromosome, gene_end, gene_stable_id, gene_start, gene_strand, is_annotated, normalized_expression_level, normalized_read_count, note, probability, probeset_id, quality_score, raw_read_count, reference_sample_id, uri, verification_platform, verification_status]
--
-- exp_m -> sample [fontsize=8, label="analyzed_sample_id"]
-- exp_g -> exp_m [fontsize=8, label="analysis_id,analyzed_sample_id\n(surjective)"]
--
-- 	0	icgc_donor_id
-- 	1	project_code
-- 	2	icgc_specimen_id
-- 	3	icgc_sample_id
-- 	4	submitted_sample_id
-- 	5	analysis_id
-- 	6	gene_stable_id
-- 	7	gene_chromosome
-- 	8	gene_strand
-- 	9	gene_start
-- 	10	gene_end
-- 	11	assembly_version
-- 	12	normalized_read_count
-- 	13	raw_read_count
-- 	14	normalized_expression_level
-- 	15	fold_change
-- 	16	reference_sample_id
-- 	17	quality_score
-- 	18	probability
-- 	19	is_annotated
-- 	20	verification_status
-- 	21	verification_platform
-- 	22	probeset_id
-- 	23	gene_build_version
-- 	24	platform
-- 	25	experimental_protocol
-- 	26	base_calling_algorithm
-- 	27	alignment_algorithm
-- 	28	normalization_algorithm
-- 	29	other_analysis_algorithm
-- 	30	sequencing_strategy
-- 	31	seq_coverage
-- 	32	raw_data_repository
-- 	33	raw_data_accession
--
REGISTER 'udfs.py' using jython as udfs;
DEFINE CLEAR udfs.clear;
set default_parallel $default_parallel
exp_m = LOAD '$m_file' AS ($m_fields);
exp_p = LOAD '$p_file' AS ($p_fields);
exp_m = FILTER exp_m BY analysis_id != 'analysis_id';
exp_p = FILTER exp_p BY analysis_id != 'analysis_id';
exp_m = FOREACH exp_m GENERATE $m_normal;
exp_p = FOREACH exp_p GENERATE $p_normal;
run -param etl_run_name=$etl_run_name -param project_id=$project_id -param donor_file=$donor_file -param specimen_file=$specimen_file -param sample_file=$sample_file clinical.pig
exp_m = JOIN
    exp_m BY analyzed_sample_id,
    clinical BY analyzed_sample_id
  USING 'replicated';
exp_mXpXs = JOIN
    exp_p BY (analysis_id, analyzed_sample_id),
    exp_m BY (analysis_id, exp_m::analyzed_sample_id)
  USING 'replicated';
exp =
  FOREACH exp_mXpXs
  GENERATE
    icgc_donor_id,
    project_code,
    exp_m::analyzed_sample_id AS submitted_sample_id,
    exp_m::analysis_id,
    exp_p::gene_stable_id;
exp = DISTINCT exp;
exp = ORDER exp BY *;
STORE exp INTO '$output_dir' USING PigStorage('\t','-schema');

