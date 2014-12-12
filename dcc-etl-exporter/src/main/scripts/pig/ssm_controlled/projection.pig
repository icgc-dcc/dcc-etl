%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';
%default EMPTY_VALUE '';

DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

%declare EMPTY_OBSERVATION ['icgc_specimen_id'#'$EMPTY_VALUE','icgc_sample_id'#'$EMPTY_VALUE','matched_icgc_sample_id'#'$EMPTY_VALUE','submitted_sample_id'#'$EMPTY_VALUE','submitted_matched_sample_id'#'$EMPTY_VALUE','quality_score'#'$EMPTY_VALUE','probability'#'$EMPTY_VALUE','total_read_count'#'$EMPTY_VALUE','mutant_allele_read_count'#'$EMPTY_VALUE','verification_status'#'$EMPTY_VALUE','verification_platform'#'$EMPTY_VALUE','biological_validation_status'#'$EMPTY_VALUE','biological_validation_platform'#'$EMPTY_VALUE','platform'#'$EMPTY_VALUE','experimental_protocol'#'$EMPTY_VALUE','sequencing_strategy'#'$EMPTY_VALUE','alignment_algorithm'#'$EMPTY_VALUE','variation_calling_algorithm'#'$EMPTY_VALUE','other_analysis_algorithm'#'$EMPTY_VALUE','seq_coverage'#'$EMPTY_VALUE','raw_data_repository'#'$EMPTY_VALUE','raw_data_accession'#'$EMPTY_VALUE','initial_data_release_date'#'$EMPTY_VALUE','control_genotype'#'$EMPTY_VALUE','tumour_genotype'#'$EMPTY_VALUE','expressed_allele'#'$EMPTY_VALUE']

-- load observation
ssm = LOAD '$OBSERVATION' USING $JSON_LOADER('-nestedLoad') as document:map[];


-- filter and denormalize for ssm
tmp_ssm = foreach ssm generate
                                    ExtractId(document#'_donor_id') as donor_id:int,
                                    document#'_mutation_id' as icgc_mutation_id,
                                    document#'_donor_id' as icgc_donor_id,
                                    document#'_project_id' as project_code,
                                    document#'chromosome' as chromosome,
                                    document#'chromosome_start' as chromosome_start,
                                    document#'chromosome_end' as chromosome_end,
                                    document#'chromosome_strand' as chromosome_strand,
                                    document#'assembly_version' as assembly_version,
                                    document#'mutation_type' as mutation_type,
                                    document#'reference_genome_allele' as reference_genome_allele,
                                    document#'mutated_from_allele' as mutated_from_allele,
                                    document#'mutated_to_allele' as mutated_to_allele,
                (bag{tuple(map[])}) document#'consequence' as consequences,
                (bag{tuple(map[])}) document#'observation' as observations;

-- only open and controlled data
flatten_ssm = FOREACH tmp_ssm GENERATE donor_id..consequences, FLATTEN(((observations is null or IsEmpty(observations)) ? {($EMPTY_OBSERVATION)} : observations)) as observation;
controlled_ssm = FILTER flatten_ssm BY observation#'marking' IN ('OPEN', 'CONTROLLED');

selected_ssm = FOREACH controlled_ssm GENERATE
                                    donor_id,
                                    icgc_mutation_id,
                                    icgc_donor_id,
                                    project_code,
                                    observation#'_specimen_id' as icgc_specimen_id,
                                    observation#'_sample_id' as icgc_sample_id,
                                    observation#'_matched_sample_id' as matched_icgc_sample_id,
                                    observation#'analyzed_sample_id' as submitted_sample_id,
                                    observation#'matched_sample_id' as submitted_matched_sample_id,
                                    chromosome,
                                    chromosome_start,
                                    chromosome_end,
                                    chromosome_strand,
                                    assembly_version,
                                    mutation_type,
                                    reference_genome_allele,
                                    observation#'control_genotype' as control_genotype,
                                    observation#'tumour_genotype' as tumour_genotype,
                                    observation#'expressed_allele' as expressed_allele,
                                    mutated_from_allele,
                                    mutated_to_allele,
                                    observation#'quality_score' as quality_score,
                                    observation#'probability' as probability,
                                    observation#'total_read_count' as total_read_count,
                                    observation#'mutant_allele_read_count' as mutant_allele_read_count,
                                    observation#'verification_status' as verification_status,
                                    observation#'verification_platform' as verification_platform,
                                    observation#'biological_validation_status' as biological_validation_status,
                                    observation#'biological_validation_platform' as biological_validation_platform,
                                    consequences,
                                    observation#'platform' as platform,
                                    observation#'experimental_protocol' as experimental_protocol,
                                    observation#'sequencing_strategy' as sequencing_strategy,
                                    observation#'base_calling_algorithm' as base_calling_algorithm,
                                    observation#'alignment_algorithm' as alignment_algorithm,
                                    observation#'variation_calling_algorithm' as variation_calling_algorithm,
                                    observation#'other_analysis_algorithm' as other_analysis_algorithm,
                                    observation#'seq_coverage' as seq_coverage,
                                    observation#'raw_data_repository' as raw_data_repository,
                                    observation#'raw_data_accession' as raw_data_accession,
                                    observation#'initial_data_release_date' as initial_data_release_date;

