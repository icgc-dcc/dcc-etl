-- stsm.pig
--
-- stsm_m	[alignment_algorithm, analysis_id, analyzed_sample_id, assembly_version, base_calling_algorithm, db_xref, experimental_protocol, matched_sample_id, note, other_analysis_algorithm, platform, raw_data_accession, raw_data_repository, seq_coverage, sequencing_strategy, uri, variation_calling_algorithm]
-- stsm_p	[analysis_id, analyzed_sample_id, annotation, chr_from, chr_from_bkpt, chr_from_flanking_seq, chr_from_range, chr_from_strand, chr_to, chr_to_bkpt, chr_to_flanking_seq, chr_to_range, chr_to_strand, db_xref, evidence, interpreted_annotation, microhomology_sequence, non_templated_sequence, note, placement, probability, quality_score, sv_id, uri, variant_type, verification_platform, verification_status, zygosity]
-- stsm_s	[analysis_id, analyzed_sample_id, bkpt_from_context, bkpt_to_context, gene_affected_by_bkpt_from, gene_affected_by_bkpt_to, gene_build_version, note, placement, sv_id, transcript_affected_by_bkpt_from, transcript_affected_by_bkpt_to]
--
-- stsm_m -> sample [fontsize=8, label="analyzed_sample_id"]
-- stsm_m -> sample [fontsize=8, label="matched_sample_id -> analyzed_sample_id"]
-- stsm_p -> stsm_m [fontsize=8, label="analysis_id,analyzed_sample_id\n(surjective)"]
-- stsm_s -> stsm_p [fontsize=8, label="analysis_id,analyzed_sample_id,sv_id,placement"]
--
-- 	0	icgc_donor_id
-- 	1	project_code
-- 	2	icgc_specimen_id
-- 	3	icgc_sample_id
-- 	4	submitted_sample_id
-- 	5	submitted_matched_sample_id
-- 	6	variant_type
-- 	7	sv_id
-- 	8	placement
-- 	9	annotation
-- 	10	interpreted_annotation
-- 	11	chr_from
-- 	12	chr_from_bkpt
-- 	13	chr_from_strand
-- 	14	chr_from_range
-- 	15	chr_from_flanking_seq
-- 	16	chr_to
-- 	17	chr_to_bkpt
-- 	18	chr_to_strand
-- 	19	chr_to_range
-- 	20	chr_to_flanking_seq
-- 	21	assembly_version
-- 	22	sequencing_strategy
-- 	23	microhomology_sequence
-- 	24	non_templated_sequence
-- 	25	evidence
-- 	26	quality_score
-- 	27	probability
-- 	28	zygosity
-- 	29	verification_status
-- 	30	verification_platform
-- 	31	gene_affected_by_bkpt_from
-- 	32	gene_affected_by_bkpt_to
-- 	33	transcript_affected_by_bkpt_from
-- 	34	transcript_affected_by_bkpt_to
-- 	35	bkpt_from_context
-- 	36	bkpt_to_context
-- 	37	gene_build_version
-- 	38	platform
-- 	39	experimental_protocol
-- 	40	base_calling_algorithm
-- 	41	alignment_algorithm
-- 	42	variation_calling_algorithm
-- 	43	other_analysis_algorithm
-- 	44	seq_coverage
-- 	45	raw_data_repository
-- 	46	raw_data_accession
--
REGISTER 'udfs.py' using jython as udfs;
DEFINE CLEAR udfs.clear;
set default_parallel $default_parallel
stsm_m = LOAD '$m_file' AS ($m_fields);
stsm_p = LOAD '$p_file' AS ($p_fields);
stsm_s = LOAD '$s_file' AS ($s_fields);
stsm_m = FILTER stsm_m BY analysis_id != 'analysis_id';
stsm_p = FILTER stsm_p BY analysis_id != 'analysis_id';
stsm_s = FILTER stsm_s BY analysis_id != 'analysis_id';
stsm_m = FOREACH stsm_m GENERATE $m_normal;
stsm_p = FOREACH stsm_p GENERATE $p_normal;
stsm_s = FOREACH stsm_s GENERATE $s_normal;
run -param etl_run_name=$etl_run_name -param project_id=$project_id -param donor_file=$donor_file -param specimen_file=$specimen_file -param sample_file=$sample_file clinical.pig
stsm_m = JOIN
    stsm_m BY analyzed_sample_id,
    clinical BY analyzed_sample_id
  USING 'replicated';
stsm_pXs = JOIN
  stsm_p BY (analysis_id, analyzed_sample_id, sv_id, placement) LEFT OUTER,
  stsm_s BY (analysis_id, analyzed_sample_id, sv_id, placement);
stsm_mXpXs = JOIN
    stsm_pXs BY (stsm_p::analysis_id, stsm_p::analyzed_sample_id),
    stsm_m BY (analysis_id, stsm_m::analyzed_sample_id)
  USING 'replicated';
stsm =
  FOREACH stsm_mXpXs
  GENERATE
    icgc_donor_id,
    project_code,
    stsm_m::analyzed_sample_id AS submitted_sample_id,
    stsm_m::matched_sample_id AS submitted_matched_sample_id,
    stsm_p::sv_id,
    stsm_p::placement,
    stsm_s::gene_affected_by_bkpt_from,
    stsm_s::gene_affected_by_bkpt_to;
stsm = DISTINCT stsm;
stsm = ORDER stsm BY *;
STORE stsm INTO '$output_dir' USING PigStorage('\t','-schema');

