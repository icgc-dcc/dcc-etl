%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';
%default OBSERVATION '<observation_files>';
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

-- load observation
observation = LOAD '$OBSERVATION' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as document:map[];

stsm = FILTER observation BY document#'_type' == 'stsm';
selected_stsm = foreach stsm generate 
                                      ExtractId(document#'_donor_id') as donor_id:int,
			   	      document#'_donor_id' as icgc_donor_id,
                                      document#'_project_id' as project_code,
                                      document#'_specimen_id' as icgc_specimen_id,
                                      document#'_sample_id' as icgc_sample_id,
				      document#'analyzed_sample_id' as submitted_sample_id, 
				      document#'matched_sample_id' as submitted_matched_sample_id,
                                      document#'variant_type ' as variant_type,
                                      document#'sv_id' as sv_id,
                                      document#'placement' as placement,
                                      document#'annotation' as annotation,
                                      document#'interpreted_annotation' as interpreted_annotation,
                                      document#'chr_from' as chr_from,
                                      document#'chr_from_bkpt' as chr_from_bkpt,
                                      document#'chr_from_strand' as chr_from_strand,
                                      document#'chr_from_range' as chr_from_range,
                                      document#'chr_from_flanking_seq' as chr_from_flanking_seq,
                                      document#'chr_to' as chr_to,
                                      document#'chr_to_bkpt' as chr_to_bkpt,
                                      document#'chr_to_strand' as chr_to_strand,
                                      document#'chr_to_range' as chr_to_range,
                                      document#'chr_to_flanking_seq' as chr_to_flanking_seq,
                                      document#'assembly_version' as assembly_version,
                                      document#'sequencing_strategy' as sequencing_strategy,
                                      document#'microhomology_sequence' as microhomology_sequence,
                                      document#'non_templated_sequence' as non_templated_sequence,
                                      document#'evidence' as evidence,
                                      document#'quality_score' as quality_score,
                                      document#'probability' as probability,
                                      document#'zygosity' as zygosity,
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
