-- ===========================================================================
/*
  Usage:
   pig \
     -param run_name=load-prod-06e-32-22 \
     -param run_number=0 \
     -param project_id=LICA-FR \
     -param donor_file=fr__07__038__donor__20130618.txt \
     -param specimen_file=fr__07__038__specimen__20130618.txt \
     -param sample_file=fr__07__038__sample__20130618.txt \
     -param ssm_m_file=ssm__fr__07__038__m__60__20130621.txt \
     -param ssm_p_file=ssm__fr__07__038__p__60__20130621.txt \
     -param ssm_s_file=ssm__fr__07__038__s__60__20130621.txt \
    ssm.pig
*/

%default run_name 'load-prod-06e-32-22';
%default run_number '0';
%default project_id 'LICA-FR';
%default donor_file 'fr__07__038__donor__20130618.txt';
%default specimen_file 'fr__07__038__specimen__20130618.txt';
%default sample_file 'fr__07__038__sample__20130618.txt';
%default ssm_m_file 'ssm__fr__07__038__m__60__20130621.txt';
%default ssm_p_file 'ssm__fr__07__038__p__60__20130621.txt';
%default ssm_s_file 'ssm__fr__07__038__s__60__20130621.txt';

REGISTER 'udfs.py' using jython as udfs;
DEFINE TRUNCATE_MUTATION udfs.truncate_mutation;
DEFINE CLEAR udfs.clear;

-- ===========================================================================

-- Load ssm files
ssm_m =
  LOAD '/icgc/normalizer/$run_name/$project_id/$ssm_m_file'
  AS (analysis_id: chararray, analyzed_sample_id: chararray, matched_sample_id: chararray, assembly_version: chararray, platform: chararray, experimental_protocol: chararray, base_calling_algorithm: chararray, alignment_algorithm: chararray, variation_calling_algorithm: chararray, other_analysis_algorithm: chararray, sequencing_strategy: chararray, seq_coverage: chararray, raw_data_repository: chararray, raw_data_accession: chararray, uri: chararray, db_xref: chararray, note: chararray);
ssm_p =
  LOAD '/icgc/normalizer/$run_name/$project_id/$ssm_p_file'
  AS (analysis_id: chararray, analyzed_sample_id: chararray, mutation_id: chararray, mutation_type: chararray, chromosome: chararray, chromosome_start: chararray, chromosome_end: chararray, chromosome_strand: chararray, refsnp_allele: chararray, refsnp_strand: chararray, reference_genome_allele: chararray, control_genotype: chararray, tumour_genotype: chararray, mutation: chararray, expressed_allele: chararray, quality_score: chararray, probability: chararray, read_count: chararray, is_annotated: chararray, verification_status: chararray, verification_platform: chararray, xref_ensembl_var_id: chararray, uri: chararray, db_xref: chararray, note: chararray);
ssm_s =
  LOAD '/icgc/normalizer/$run_name/$project_id/$ssm_s_file'
  AS (analysis_id: chararray, analyzed_sample_id: chararray, mutation_id: chararray, consequence_type: chararray, aa_mutation: chararray, cds_mutation: chararray, protein_domain_affected: chararray, gene_affected: chararray, transcript_affected: chararray, gene_build_version: chararray, note: chararray);


-- Filter out headers (using PK field names for now)
ssm_m =
  FILTER ssm_m
  BY analysis_id != 'analysis_id';
ssm_p =
  FILTER ssm_p
  BY analysis_id != 'analysis_id';
ssm_s =
  FILTER ssm_s
  BY analysis_id != 'analysis_id';


-- Project fields of interest
ssm_m =
  FOREACH ssm_m
  GENERATE
    TRIM(CLEAR(analysis_id)) AS analysis_id,
    TRIM(CLEAR(analyzed_sample_id)) AS analyzed_sample_id,
    TRIM(CLEAR(matched_sample_id)) AS matched_sample_id,
    TRIM(CLEAR(assembly_version)) AS assembly_version;
ssm_p =
  FOREACH ssm_p
  GENERATE
    TRIM(CLEAR(analysis_id)) AS analysis_id,
    TRIM(CLEAR(analyzed_sample_id)) AS analyzed_sample_id,
    TRIM(CLEAR(mutation_id)) AS mutation_id,
    TRIM(CLEAR(mutation_type)) AS mutation_type,
    TRIM(CLEAR(chromosome)) AS chromosome,
    TRIM(CLEAR(chromosome_start)) AS chromosome_start,
    TRIM(CLEAR(chromosome_end)) AS chromosome_end,
    TRIM(CLEAR(chromosome_strand)) AS chromosome_strand,
    TRIM(CLEAR(reference_genome_allele)) AS reference_genome_allele,
    TRIM(CLEAR(control_genotype)) AS control_genotype,
    TRIM(CLEAR(tumour_genotype)) AS tumour_genotype,
    TRUNCATE_MUTATION(
      TRIM(CLEAR(mutation))) AS mutation;
ssm_s =
  FOREACH ssm_s
  GENERATE
    TRIM(CLEAR(analysis_id)) AS analysis_id,
    TRIM(CLEAR(analyzed_sample_id)) AS analyzed_sample_id,
    TRIM(CLEAR(mutation_id)) AS mutation_id,
    TRIM(CLEAR(gene_affected)) AS gene_affected;


-- Join files
ssm_mXp =
  JOIN
    ssm_m BY (analysis_id, analyzed_sample_id),
    ssm_p BY (analysis_id, analyzed_sample_id);
ssm_mXp =
  FOREACH ssm_mXp
  GENERATE ssm_p::analysis_id, ssm_p::analyzed_sample_id, matched_sample_id, assembly_version, mutation_id, mutation_type, chromosome, chromosome_start, chromosome_end, chromosome_strand, reference_genome_allele, control_genotype, tumour_genotype, mutation;
ssm_mXpXs =
  JOIN
    ssm_mXp BY (analysis_id, analyzed_sample_id, mutation_id) LEFT OUTER, -- This one MUST be a left join
    ssm_s BY (analysis_id, analyzed_sample_id, mutation_id);
ssm =
  FOREACH ssm_mXpXs
  GENERATE ssm_p::analysis_id, ssm_p::analyzed_sample_id, ssm_p::mutation_id, matched_sample_id, assembly_version, mutation_type, chromosome, chromosome_start, chromosome_end, chromosome_strand, reference_genome_allele, control_genotype, tumour_genotype, mutation, gene_affected;


-- ===========================================================================
-- Grab original donor_id (ssm data only contains sample ID information, yet we need the original donor ID in order to get the icgc_donor_id)

run -param run_name=$run_name -param project_id=$project_id -param donor_file=$donor_file -param specimen_file=$specimen_file -param sample_file=$sample_file clinical.legacy.pig -- defines donorXsample; switch to using param file? TODO

ssm =
  JOIN
    ssm BY analyzed_sample_id,
    donorXsample BY analyzed_sample_id -- still comparatively small
  USING 'replicated';
ssm =
  FOREACH ssm
  GENERATE donor_id, analysis_id, ssm_p::analyzed_sample_id, mutation_id, matched_sample_id, assembly_version, mutation_type, chromosome, chromosome_start, chromosome_end, chromosome_strand, reference_genome_allele, control_genotype, tumour_genotype, mutation, gene_affected;

-- ===========================================================================
-- translate codelists -- TODO: move upstream
mutation_type_codec =
  LOAD '/icgc/meta/codelists/ssm_mutation_type_lookup.tsv'
  AS (code, value);

ssm =
  JOIN
    ssm BY mutation_type LEFT OUTER, -- left join so as to not discard anything
    mutation_type_codec BY code
  USING 'replicated';

ssm =
  FOREACH ssm
  GENERATE donor_id, analysis_id, analyzed_sample_id, mutation_id, matched_sample_id, assembly_version, mutation_type_codec::value AS mutation_type, chromosome, chromosome_start, chromosome_end, chromosome_strand, reference_genome_allele, control_genotype, tumour_genotype, mutation, gene_affected;

-- ---------------------------------------------------------------------------
assembly_version_codec =
  LOAD '/icgc/meta/codelists/assembly_version_lookup.tsv'
  AS (code, value);

ssm =
  JOIN
    ssm BY assembly_version LEFT OUTER, -- left join so as to not discard anything
    assembly_version_codec BY code
  USING 'replicated';

ssm =
  FOREACH ssm
  GENERATE donor_id, analysis_id, analyzed_sample_id, mutation_id, matched_sample_id, assembly_version_codec::value AS assembly_version, mutation_type, chromosome, chromosome_start, chromosome_end, chromosome_strand, reference_genome_allele, control_genotype, tumour_genotype, mutation, gene_affected;

-- ---------------------------------------------------------------------------
chromosome_codec =
  LOAD '/icgc/meta/codelists/chromosome_lookup.tsv'
  AS (code, value);

ssm =
  JOIN
    ssm BY chromosome LEFT OUTER, -- left join so as to not discard anything
    chromosome_codec BY code
  USING 'replicated';

ssm =
  FOREACH ssm
  GENERATE donor_id, analysis_id, analyzed_sample_id, mutation_id, matched_sample_id, assembly_version, mutation_type, chromosome_codec::value AS chromosome, chromosome_start, chromosome_end, chromosome_strand, reference_genome_allele, control_genotype, tumour_genotype, mutation, gene_affected;


-- ===========================================================================
-- import project
ssm =
  FOREACH ssm
  GENERATE '$project_id' AS project_code, donor_id, analysis_id, analyzed_sample_id, mutation_id, matched_sample_id, assembly_version, mutation_type, chromosome, chromosome_start, chromosome_end, chromosome_strand, reference_genome_allele, control_genotype, tumour_genotype, mutation, gene_affected;

-- ===========================================================================
-- get icgc donor ID; expects project to be inserted already
donor_lookup =
  LOAD '/icgc/meta/identification/donor_lookup.tsv'
  AS (id, donor_id, project_id);

ssm =
  JOIN
    ssm BY (donor_id, project_code) LEFT OUTER, -- left join so as to not discard anything
    donor_lookup BY (donor_id, project_id);

ssm =
  FOREACH ssm
  GENERATE CONCAT('DO', donor_lookup::id) AS icgc_donor_id, project_code, donor::donor_id, analysis_id, analyzed_sample_id, mutation_id, matched_sample_id, assembly_version, mutation_type, chromosome, chromosome_start, chromosome_end, chromosome_strand, reference_genome_allele, control_genotype, tumour_genotype, mutation, gene_affected;

-- ---------------------------------------------------------------------------
-- get icgc mutation ID; expects mutation_type to be translated already
mutation_lookup =
  LOAD '/icgc/meta/identification/mutation_lookup.tsv'
  AS (id, chromosome, chromosome_start, chromosome_end, mutation, mutation_type, assembly_version); -- example: 952185	7	150934813	150934813	G>T	single base substitution	GRCh37

ssm =
  JOIN
    ssm BY (chromosome, chromosome_start, chromosome_end, mutation, mutation_type, assembly_version) LEFT OUTER, -- left join so as to not discard anything
    mutation_lookup BY (chromosome, chromosome_start, chromosome_end, mutation, mutation_type, assembly_version);

ssm =
  FOREACH ssm
  GENERATE
    CONCAT('MU', mutation_lookup::id)
    AS icgc_mutation_id,
    icgc_donor_id,
    project_code,
    analyzed_sample_id AS submitted_sample_id,
    mutation_id,
    matched_sample_id AS submitted_matched_sample_id,
    ssm::assembly_version,
    ssm::mutation_type,
    ssm::chromosome,
    ssm_p::chromosome_start,
    ssm_p::chromosome_end,
    chromosome_strand,
    reference_genome_allele,
    control_genotype,
    tumour_genotype,
    ssm_p::mutation,
    gene_affected;

-- ===========================================================================

STORE ssm
  INTO '/icgc/testing/$run_name-$run_number/$project_id.ssm'
  USING PigStorage('\t','-schema');

-- ===========================================================================
