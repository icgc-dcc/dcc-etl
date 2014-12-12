%default OBSERVATION '/icgc/etl/r-53049_0-965_0_all/cnsm.json/part-*';
%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';

DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

-- load observation
observation = LOAD '$OBSERVATION' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as document:map[];

cnsm = FILTER observation BY document#'_type' == '$DATATYPE';
selected_cnsm = foreach cnsm generate 
                                      ExtractId(document#'_donor_id') as donor_id:int,
				      document#'_donor_id' as icgc_donor_id, 
                                      document#'_project_id' as project_code, 
                                      document#'_specimen_id' as icgc_specimen_id, 
                                      document#'_sample_id' as icgc_sample_id, 
                                      document#'_matched_sample_id' as matched_icgc_sample_id, 
                                      document#'analyzed_sample_id' as submitted_sample_id, 
                                      document#'matched_sample_id' as submitted_matched_sample_id,
                                      document#'mutation_type' as mutation_type,
                                      document#'copy_number' as copy_number,
                                      document#'segment_mean' as segment_mean,
                                      document#'segment_median' as segment_median,
                                      document#'chromosome' as chromosome,
                                      document#'chromosome_start' as chromosome_start,
                                      document#'chromosome_end' as chromosome_end,
                                      document#'assembly_version' as assembly_version,
                                      document#'chromosome_start_range' as chromosome_start_range,
                                      document#'chromosome_end_range' as chromosome_end_range,
                                      document#'start_probe_id' as start_probe_id,
                                      document#'end_probe_id' as end_probe_id,
                                      document#'sequencing_strategy' as sequencing_strategy,
                                      document#'quality_score' as quality_score,
                                      document#'probability' as probability,
                                      document#'is_annotated' as is_annotated,
                                      document#'verification_status' as verification_status,
                                      document#'verification_platform' as verification_platform,
                  (bag{tuple(map[])}) document#'consequence' as consequences,
                                      document#'platform' as platform,
                                      document#'experimental_protocol' as experimental_protocol,
                                      document#'base_calling_algorithm' as base_calling_algorithm,
                                      document#'alignment_algorithm' as alignment_algorithm,
                                      document#'variation_calling_algorithm' as variation_calling_algorithm,
                                      document#'other_analysis_algorithm' as other_analysis_algorithm,
                                      document#'seq_coverage' as seq_coverage,
                                      document#'raw_data_repository' as raw_data_repository,
                                      document#'raw_data_accession' as raw_data_accession;
