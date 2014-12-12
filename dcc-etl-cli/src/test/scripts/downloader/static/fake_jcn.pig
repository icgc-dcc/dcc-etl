-- jcn:
--
-- jcn_m	[alignment_algorithm, analysis_id, analyzed_sample_id, assembly_version, base_calling_algorithm, experimental_protocol, gene_build_version, normalization_algorithm, note, other_analysis_algorithm, platform, raw_data_accession, raw_data_repository, seq_coverage, sequencing_strategy]
-- jcn_p	[analysis_id, analyzed_sample_id, db_xref, exon1_chromosome, exon1_end, exon1_number_bases, exon1_strand, exon2_chromosome, exon2_number_bases, exon2_start, exon2_strand, gene_chromosome, gene_end, gene_stable_id, gene_start, gene_strand, is_fusion_gene, is_novel_splice_form, junction_id, junction_read_count, junction_seq, junction_type, note, probability, quality_score, second_gene_stable_id, uri, verification_platform, verification_status]
--
-- jcn_m -> sample [fontsize=8, label="analyzed_sample_id"]
-- jcn_p -> jcn_m [fontsize=8, label="analysis_id,analyzed_sample_id\n(surjective)"]
--
--	 0       icgc_donor_id
--	 1       project_code
--	 2       icgc_specimen_id
--	 3       icgc_sample_id
--	 4       submitted_sample_id
--	 5       analysis_id
--	 6       junction_id
--	 7       gene_stable_id
--	 8       gene_chromosome
--	 9       gene_strand
--	 10      gene_start
--	 11      gene_end
--	 12      assembly_version
--	 13      second_gene_stable_id
--	 14      exon1_chromosome
--	 15      exon1_number_bases
--	 16      exon1_end
--	 17      exon1_strand
--	 18      exon2_chromosome
--	 19      exon2_number_bases
--	 20      exon2_start
--	 21      exon2_strand
--	 22      is_fusion_gene
--	 23      is_novel_splice_form
--	 24      junction_seq
--	 25      junction_type
--	 26      junction_read_count
--	 27      quality_score
--	 28      probability
--	 29      verification_status
--	 30      verification_platform
--	 31      gene_build_version
--	 32      platform
--	 33      experimental_protocol
--	 34      base_calling_algorithm
--	 35      alignment_algorithm
--	 36      normalization_algorithm
--	 37      other_analysis_algorithm
--	 38      sequencing_strategy
--	 39      seq_coverage
--	 40      raw_data_repository
--	 41      raw_data_accession
--
REGISTER 'udfs.py' using jython as udfs;
DEFINE CLEAR udfs.clear;
set default_parallel $default_parallel
jcn_m = LOAD '$m_file' AS ($m_fields);
jcn_p = LOAD '$p_file' AS ($p_fields);
jcn_m = FILTER jcn_m BY analysis_id != 'analysis_id';
jcn_p = FILTER jcn_p BY analysis_id != 'analysis_id';
jcn_m = FOREACH jcn_m GENERATE $m_normal;
jcn_p = FOREACH jcn_p GENERATE $p_normal;
run -param etl_run_name=$etl_run_name -param project_id=$project_id -param donor_file=$donor_file -param specimen_file=$specimen_file -param sample_file=$sample_file clinical.pig
jcn_m = JOIN
    jcn_m BY analyzed_sample_id,
    clinical BY analyzed_sample_id
  USING 'replicated';
jcn_mXpXs = JOIN
    jcn_p BY (analysis_id, analyzed_sample_id),
    jcn_m BY (analysis_id, jcn_m::analyzed_sample_id)
  USING 'replicated';
jcn =
  FOREACH jcn_mXpXs
  GENERATE
    icgc_donor_id,
    project_code,
    jcn_m::analyzed_sample_id AS submitted_sample_id,
    jcn_m::analysis_id,
    jcn_p::junction_id;
jcn = DISTINCT jcn;
jcn = ORDER jcn BY *;
STORE jcn INTO '$output_dir' USING PigStorage('\t','-schema');

