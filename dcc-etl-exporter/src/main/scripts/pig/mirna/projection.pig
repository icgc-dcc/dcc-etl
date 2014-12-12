%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';
%default OBSERVATION '/icgc/etl/r-53049_0-965_0_all/mirna.json/part-*';

DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

-- load observation
observation = LOAD '$OBSERVATION' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as document:map[];

-- filter and denormalize for mirna 
mirna = FILTER observation BY document#'_type' == 'mirna';
selected_mirna = foreach mirna generate 
                                        ExtractId(document#'_donor_id') as donor_id:int,
					document#'_donor_id' as icgc_donor_id,
                                        document#'_project_id' as project_code,
                                        document#'_specimen_id' as icgc_specimen_id,
                                        document#'_sample_id' as icgc_sample_id,
				        document#'analyzed_sample_id' as submitted_sample_id, 
                                        document#'analysis_id' as analysis_id,
                                        document#'mirna_seq' as mirna_seq,
                                        document#'normalized_read_count' as normalized_read_count,
                                        document#'raw_read_count' as raw_read_count,
                                        document#'normalized_expression_level' as normalized_expression_level,
                                        document#'fold_change' as fold_change,
                                        document#'reference_sample_id' as reference_sample_id,
                                        document#'quality_score' as quality_score,
                                        document#'probability' as probability,
                                        document#'is_annotated' as is_annotated,
                                        document#'verification_status' as verification_status,
                                        document#'verification_platform' as verification_platform,
                    (bag{tuple(map[])}) document#'consequence' as consequences,
                                        document#'assembly_version' as assembly_version,
                                        document#'gene_build_version' as gene_build_version,
                                        document#'platform' as platform,
                                        document#'experimental_protocol' as experimental_protocol,
                                        document#'base_calling_algorithm' as base_calling_algorithm,
                                        document#'alignment_algorithm' as alignment_algorithm,
                                        document#'normalization_algorithm' as normalization_algorithm,
                                        document#'other_analysis_algorithm' as other_analysis_algorithm,
                                        document#'sequencing_strategy' as sequencing_strategy,
                                        document#'seq_coverage' as seq_coverage,
                                        document#'raw_data_repository' as raw_data_repository,
                                        document#'raw_data_accession' as raw_data_accession;
