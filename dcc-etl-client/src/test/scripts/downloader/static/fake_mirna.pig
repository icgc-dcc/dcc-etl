-- mirna
--
-- mirna_m	[alignment_algorithm, analysis_id, analyzed_sample_id, assembly_version, base_calling_algorithm, db_xref, experimental_protocol, gene_build_version, normalization_algorithm, note, other_analysis_algorithm, platform, raw_data_accession, raw_data_repository, seq_coverage, sequencing_strategy, uri]
-- mirna_p	[analysis_id, analyzed_sample_id, db_xref, fold_change, is_annotated, mirna_seq, normalized_expression_level, normalized_read_count, note, probability, quality_score, raw_read_count, reference_sample_id, uri, verification_platform, verification_status]
-- mirna_s	[chromosome, chromosome_end, chromosome_start, chromosome_strand, mirna_seq, note, xref_mirbase_id]
--
-- mirna_p -> mirna_m [fontsize=8, label="analysis_id,analyzed_sample_id\n(surjective)"]
-- mirna_s -> mirna_p [fontsize=8, label="mirna_seq"]
-- mirna_m -> sample [fontsize=8, label="analyzed_sample_id"]
--
-- 	0	icgc_donor_id
-- 	1	project_code
-- 	2	icgc_specimen_id
-- 	3	icgc_sample_id
-- 	4	submitted_sample_id
-- 	5	analysis_id
-- 	6	mirna_seq
-- 	7	normalized_read_count
-- 	8	raw_read_count
-- 	9	normalized_expression_level
-- 	10	fold_change
-- 	11	reference_sample_id
-- 	12	quality_score
-- 	13	probability
-- 	14	is_annotated
-- 	15	verification_status
-- 	16	verification_platform
-- 	17	chromosome
-- 	18	chromosome_start
-- 	19	chromosome_end
-- 	20	chromosome_strand
-- 	21	assembly_version
-- 	22	xref_mirbase_id
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
mirna_m = LOAD '$m_file' AS ($m_fields);
mirna_p = LOAD '$p_file' AS ($p_fields);
mirna_s = LOAD '$s_file' AS ($s_fields);
mirna_m = FILTER mirna_m BY analysis_id != 'analysis_id';
mirna_p = FILTER mirna_p BY analysis_id != 'analysis_id';
mirna_s = FILTER mirna_s BY mirna_seq != 'mirna_seq';
mirna_m = FOREACH mirna_m GENERATE $m_normal;
mirna_p = FOREACH mirna_p GENERATE $p_normal;
mirna_s = FOREACH mirna_s GENERATE $s_normal;
run -param etl_run_name=$etl_run_name -param project_id=$project_id -param donor_file=$donor_file -param specimen_file=$specimen_file -param sample_file=$sample_file clinical.pig
mirna_m = JOIN
    mirna_m BY analyzed_sample_id,
    clinical BY analyzed_sample_id
  USING 'replicated';
mirna_pXs = JOIN
  mirna_p BY mirna_seq LEFT OUTER,
  mirna_s BY mirna_seq;
mirna_mXpXs = JOIN
    mirna_pXs BY (mirna_p::analysis_id, mirna_p::analyzed_sample_id),
    mirna_m BY (analysis_id, mirna_m::analyzed_sample_id)
  USING 'replicated';
mirna =
  FOREACH mirna_mXpXs
  GENERATE
    icgc_donor_id,
    project_code,
    mirna_m::analyzed_sample_id AS submitted_sample_id,
    mirna_m::analysis_id,
    mirna_p::mirna_seq,
    mirna_s::xref_mirbase_id;
mirna = DISTINCT mirna;
mirna = ORDER mirna BY *;
STORE mirna INTO '$output_dir' USING PigStorage('\t','-schema');
