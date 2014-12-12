%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';
%default OBSERVATION '/icgc/etl/r-22-projects2_all/sgv.json/part-*';
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

-- load observation
sgv = LOAD '$OBSERVATION' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as document:map[];

selected_sgv = foreach sgv generate 
                                    ExtractId(document#'_donor_id') as donor_id:int,
				    document#'_variant_id' as icgc_variant_id,
                                    document#'_donor_id' as icgc_donor_id,
                                    document#'_project_id' as project_code,
                                    document#'_specimen_id' as icgc_specimen_id,
                                    document#'_sample_id' as icgc_sample_id,
                                    document#'analyzed_sample_id' as submitted_sample_id,
                                    document#'analysis_id' as analysis_id,
                                    document#'chromosome' as chromosome,
                                    document#'chromosome_start' as chromosome_start,
                                    document#'chromosome_end' as chromosome_end,
                                    document#'chromosome_strand' as chromosome_strand,
                                    document#'assembly_version' as assembly_version,
                                    document#'variant_type' as variant_type,
                                    document#'reference_genome_allele' as reference_genome_allele,
                                    document#'genotype' as genotype,
                                    document#'variant_allele' as variant_allele,
                                    document#'quality_score' as quality_score,
                                    document#'probability' as probability,
                                    document#'total_read_count' as total_read_count,
                                    document#'variant_allele_read_count' as variant_allele_read_count,
                                    document#'verification_status' as verification_status,
                                    document#'verification_platform' as verification_platform,
                                    (bag{tuple(map[])}) document#'consequence' as consequences,
                                    document#'platform' as platform,
                                    document#'experimental_protocol' as experimental_protocol,
                                    document#'base_calling_algorithm' as base_calling_algorithm,
                                    document#'alignment_algorithm' as alignment_algorithm,
                                    document#'variation_calling_algorithm' as variation_calling_algorithm,
                                    document#'other_analysis_algorithm' as other_analysis_algorithm,
                                    document#'sequencing_strategy' as sequencing_strategy,
                                    document#'seq_coverage' as seq_coverage,
                                    document#'raw_data_repository' as raw_data_repository,
                                    document#'raw_data_accession' as raw_data_accession,
                                    document#'note' as note;
