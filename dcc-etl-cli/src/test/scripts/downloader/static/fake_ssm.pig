-- ===========================================================================
-- TODO:
-- - ensure no WARN from casts
-- - get ID from db
-- - filter out comments in codecs though it's inconsequential (improvement)
-- - un-MapReduce the output? (improvement)

-- ===========================================================================

-- dictionary:
--
-- ssm_m	[alignment_algorithm, analysis_id, analyzed_sample_id, assembly_version, base_calling_algorithm, db_xref, experimental_protocol, matched_sample_id, note, other_analysis_algorithm, platform, raw_data_accession, raw_data_repository, seq_coverage, sequencing_strategy, uri, variation_calling_algorithm]
-- ssm_p	[analysis_id, analyzed_sample_id, chromosome, chromosome_end, chromosome_start, chromosome_strand, control_genotype, db_xref, expressed_allele, is_annotated, mutation, mutation_id, mutation_type, note, probability, quality_score, read_count, reference_genome_allele, refsnp_allele, refsnp_strand, tumour_genotype, uri, verification_platform, verification_status, xref_ensembl_var_id]
-- ssm_s	[aa_mutation, analysis_id, analyzed_sample_id, cds_mutation, consequence_type, gene_affected, gene_build_version, mutation_id, note, protein_domain_affected, transcript_affected]
--
-- ssm_m -> sample [fontsize=8, label="analyzed_sample_id"]
-- ssm_m -> sample [fontsize=8, label="matched_sample_id -> analyzed_sample_id"]
-- ssm_p -> ssm_m [fontsize=8, label="analysis_id,analyzed_sample_id\n(surjective)"]
-- ssm_s -> ssm_p [fontsize=8, label="analysis_id,analyzed_sample_id,mutation_id"]
--

-- ---------------------------------------------------------------------------

-- static:
-- 	0	icgc_mutation_id
-- 	1	icgc_donor_id
-- 	2	project_code
-- 	3	icgc_specimen_id
-- 	4	icgc_sample_id
-- 	5	matched_icgc_sample_id
-- 	6	submitted_sample_id
-- 	7	submitted_matched_sample_id
-- 	8	chromosome
-- 	9	chromosome_start
-- 	10	chromosome_end
-- 	11	chromosome_strand
-- 	12	assembly_version
-- 	13	mutation_type
-- 	14	mutation
-- 	15	reference_genome_allele
-- 	16	control_genotype
-- 	17	tumour_genotype
-- 	18	expressed_allele
-- 	19	refsnp_allele
-- 	20	refsnp_strand
-- 	21	quality_score
-- 	22	probability
-- 	23	read_count
-- 	24	is_annotated
-- 	25	verification_status
-- 	26	verification_platform
-- 	27	xref_ensembl_var_id
-- 	28	consequence_type
-- 	29	aa_mutation
-- 	30	cds_mutation
-- 	31	protein_domain_affected
-- 	32	gene_affected
-- 	33	transcript_affected
-- 	34	gene_build_version
-- 	35	platform
-- 	36	experimental_protocol
-- 	37	sequencing_strategy
-- 	38	base_calling_algorithm
-- 	39	alignment_algorithm
-- 	40	variation_calling_algorithm
-- 	41	other_analysis_algorithm
-- 	42	seq_coverage
-- 	43	raw_data_repository
-- 	44	raw_data_accession
--

REGISTER 'udfs.py' using jython as udfs;
DEFINE TRUNCATE_MUTATION udfs.truncate_mutation;
DEFINE CLEAR udfs.clear;
set default_parallel $default_parallel

-- ===========================================================================

-- Load ssm files
ssm_m = LOAD '$m_file' AS ($m_fields);
ssm_p = LOAD '$p_file' AS ($p_fields);
ssm_s = LOAD '$s_file' AS ($s_fields);

-- Filter out headers
ssm_m =
  FILTER ssm_m
  BY analysis_id != 'analysis_id';
ssm_p =
  FILTER ssm_p
  BY analysis_id != 'analysis_id';
ssm_s =
  FILTER ssm_s
  BY analysis_id != 'analysis_id';

-- Pre-process fields (casts, trim, translate missing codes, ...)
ssm_m = FOREACH ssm_m GENERATE $m_normal;
ssm_p = FOREACH ssm_p GENERATE $p_normal;
ssm_s = FOREACH ssm_s GENERATE $s_normal;

ssm_p = FOREACH ssm_p GENERATE TRUNCATE_MUTATION(mutation) AS truncated_mutation, *;


-- ===========================================================================
-- translate codelists

assembly_version_codec =
  LOAD '/icgc/meta/codelists/assembly_version_lookup.tsv'
  AS (code: chararray, assembly_version: chararray);
ssm_m =
  JOIN
    ssm_m BY assembly_version LEFT OUTER, -- left join so as to not discard anything
    assembly_version_codec BY code
  USING 'replicated';

mutation_type_codec =
  LOAD '/icgc/meta/codelists/ssm_mutation_type_lookup.tsv'
  AS (code: chararray, mutation_type: chararray);
ssm_p =
  JOIN
    ssm_p BY mutation_type LEFT OUTER, -- left join so as to not discard anything
    mutation_type_codec BY code
  USING 'replicated';

chromosome_codec =
  LOAD '/icgc/meta/codelists/chromosome_lookup.tsv'
  AS (code: chararray, chromosome: chararray);
ssm_p =
  JOIN
    ssm_p BY chromosome LEFT OUTER, -- left join so as to not discard anything
    chromosome_codec BY code
  USING 'replicated';

-- ===========================================================================
-- Grab original donor_id (ssm data only contains sample ID information, yet we need the original donor ID in order to get the icgc_donor_id)
-- Also import the project code
run -param etl_run_name=$etl_run_name -param project_id=$project_id -param donor_file=$donor_file -param specimen_file=$specimen_file -param sample_file=$sample_file clinical.pig

-- ===========================================================================
-- Join files
ssm_m = JOIN
    ssm_m BY analyzed_sample_id,
    clinical BY analyzed_sample_id
  USING 'replicated';
ssm_pXs = JOIN
  ssm_p BY (analysis_id, analyzed_sample_id, mutation_id) LEFT OUTER,
  ssm_s BY (analysis_id, analyzed_sample_id, mutation_id);
ssm_mXpXs = JOIN
    ssm_pXs BY (ssm_p::analysis_id, ssm_p::analyzed_sample_id),
    ssm_m BY (analysis_id, ssm_m::analyzed_sample_id)
  USING 'replicated';

-- ===========================================================================
-- Get icgc mutation ID; expects mutation_type to be translated already

mutation_lookup =
  LOAD '/icgc/meta/identification/mutation_lookup.tsv'
  AS (id: chararray, chromosome: chararray, chromosome_start: chararray, chromosome_end: chararray, mutation: chararray, mutation_type: chararray, assembly_version: chararray); -- example: 952185	7	150934813	150934813	G>T	single base substitution	GRCh37
mutation_lookup = FOREACH mutation_lookup GENERATE CONCAT('MU', id) AS icgc_mutation_id, *;
ssm =
  JOIN
    ssm_mXpXs BY (chromosome_codec::chromosome, chromosome_start, chromosome_end, ssm_p::mutation, mutation_type_codec::mutation_type, assembly_version_codec::assembly_version) LEFT OUTER, -- left join so as to not discard anything
    mutation_lookup BY (chromosome, chromosome_start, chromosome_end, mutation, mutation_type, assembly_version);


-- ===========================================================================
-- Project fields of interest

ssm =
  FOREACH ssm
  GENERATE
    icgc_mutation_id,
    icgc_donor_id,
    project_code,
    ssm_m::analyzed_sample_id AS submitted_sample_id,
    ssm_m::matched_sample_id AS submitted_matched_sample_id,
    chromosome_codec::chromosome,
    ssm_p::chromosome_start,
    ssm_p::chromosome_end,
    chromosome_strand,
    assembly_version_codec::assembly_version,
    mutation_type_codec::mutation_type,
    truncated_mutation AS mutation,
    reference_genome_allele,
    control_genotype,
    tumour_genotype,
    gene_affected;

-- ===========================================================================

ssm = DISTINCT ssm;
ssm = ORDER ssm BY *; -- TODO: may be unnecessary
STORE ssm INTO '$output_dir' USING PigStorage('\t','-schema');

-- ===========================================================================

