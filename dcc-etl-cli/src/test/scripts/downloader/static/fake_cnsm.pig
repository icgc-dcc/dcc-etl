-- cnsm
--
-- cnsm_m	[alignment_algorithm, analysis_id, analyzed_sample_id, assembly_version, base_calling_algorithm, db_xref, experimental_protocol, matched_sample_id, note, other_analysis_algorithm, platform, raw_data_accession, raw_data_repository, seq_coverage, sequencing_strategy, uri, variation_calling_algorithm]
-- cnsm_p	[analysis_id, analyzed_sample_id, chromosome, chromosome_end, chromosome_end_range, chromosome_start, chromosome_start_range, copy_number, db_xref, end_probe_id, is_annotated, mutation_id, mutation_type, note, probability, quality_score, segment_mean, segment_median, start_probe_id, uri, verification_platform, verification_status]
-- cnsm_s	[analysis_id, analyzed_sample_id, gene_affected, gene_build_version, mutation_id, note, transcript_affected]
--
-- cnsm_p -> cnsm_m [fontsize=8, label="analysis_id,analyzed_sample_id\n(surjective)"]
-- cnsm_s -> cnsm_p [fontsize=8, label="analysis_id,analyzed_sample_id,mutation_id"]
-- cnsm_m -> sample [fontsize=8, label="analyzed_sample_id"]
-- cnsm_m -> sample [fontsize=8, label="matched_sample_id -> analyzed_sample_id"]
--
-- 	0	icgc_donor_id
-- 	1	project_code
-- 	2	icgc_specimen_id
-- 	3	icgc_sample_id
-- 	4	matched_icgc_sample_id
-- 	5	submitted_sample_id
-- 	6	submitted_matched_sample_id
-- 	7	mutation_type
-- 	8	copy_number
-- 	9	segment_mean
-- 	10	segment_median
-- 	11	chromosome
-- 	12	chromosome_start
-- 	13	chromosome_end
-- 	14	assembly_version
-- 	15	chromosome_start_range
-- 	16	chromosome_end_range
-- 	17	start_probe_id
-- 	18	end_probe_id
-- 	19	sequencing_strategy
-- 	20	quality_score
-- 	21	probability
-- 	22	is_annotated
-- 	23	verification_status
-- 	24	verification_platform
-- 	25	gene_affected
-- 	26	transcript_affected
-- 	27	gene_build_version
-- 	28	platform
-- 	29	experimental_protocol
-- 	30	base_calling_algorithm
-- 	31	alignment_algorithm
-- 	32	variation_calling_algorithm
-- 	33	other_analysis_algorithm
-- 	34	seq_coverage
-- 	35	raw_data_repository
-- 	36	raw_data_accession
--
REGISTER 'udfs.py' using jython as udfs;
DEFINE CLEAR udfs.clear;
set default_parallel $default_parallel
cnsm_m = LOAD '$m_file' AS ($m_fields);
cnsm_p = LOAD '$p_file' AS ($p_fields);
cnsm_s = LOAD '$s_file' AS ($s_fields);
cnsm_m = FILTER cnsm_m BY analysis_id != 'analysis_id';
cnsm_p = FILTER cnsm_p BY analysis_id != 'analysis_id';
cnsm_s = FILTER cnsm_s BY analysis_id != 'analysis_id';
cnsm_m = FOREACH cnsm_m GENERATE $m_normal;
cnsm_p = FOREACH cnsm_p GENERATE $p_normal;
cnsm_s = FOREACH cnsm_s GENERATE $s_normal;
mutation_type_codec =
  LOAD '/icgc/meta/codelists/cnsm_mutation_type_lookup.tsv'
  AS (code: chararray, mutation_type: chararray);
mutation_type_codec = FOREACH mutation_type_codec GENERATE (chararray)code, (chararray)mutation_type;
cnsm_p =
  JOIN
    cnsm_p BY mutation_type LEFT OUTER,
    mutation_type_codec BY code
  USING 'replicated';
run -param etl_run_name=$etl_run_name -param project_id=$project_id -param donor_file=$donor_file -param specimen_file=$specimen_file -param sample_file=$sample_file clinical.pig
cnsm_m = JOIN
    cnsm_m BY analyzed_sample_id,
    clinical BY analyzed_sample_id
  USING 'replicated';
cnsm_pXs = JOIN
  cnsm_p BY (analysis_id, analyzed_sample_id, mutation_id) LEFT OUTER,
  cnsm_s BY (analysis_id, analyzed_sample_id, mutation_id);
cnsm_mXpXs = JOIN
    cnsm_pXs BY (cnsm_p::analysis_id, cnsm_p::analyzed_sample_id),
    cnsm_m BY (analysis_id, cnsm_m::analyzed_sample_id)
  USING 'replicated';
cnsm =
  FOREACH cnsm_mXpXs
  GENERATE
    icgc_donor_id,
    project_code,
    cnsm_m::analyzed_sample_id AS submitted_sample_id,
    cnsm_m::matched_sample_id AS submitted_matched_sample_id,
    mutation_type_codec::mutation_type,
    cnsm_s::gene_affected,
    cnsm_s::transcript_affected;
cnsm = DISTINCT cnsm;
cnsm = ORDER cnsm BY *;
STORE cnsm INTO '$output_dir' USING PigStorage('\t','-schema');

