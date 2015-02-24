%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';
%default OBSERVATION '<observation_files>';
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();


-- load observation
observation = LOAD '$OBSERVATION' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as document:map[];

pexp = FILTER observation BY document#'_type' == 'pexp';
selected_pexp = foreach pexp generate 
                                      ExtractId(document#'_donor_id') as donor_id:int,
				      document#'_donor_id' as icgc_donor_id,
 				      document#'_project_id' as project_code,
 				      document#'_specimen_id' as icgc_specimen_id,
 				      document#'_sample_id' as icgc_sample_id,
				      document#'analyzed_sample_id' as submitted_sample_id, 
 				      document#'analysis_id' as analysis_id,
 				      document#'antibody_id' as antibody_id,
 				      document#'gene_name' as gene_name,
 				      document#'gene_stable_id' as gene_stable_id,
 				      document#'gene_build_version' as gene_build_version,
 				      document#'normalized_expression_level' as normalized_expression_level,
 				      document#'verification_status' as verification_status,
 				      document#'verification_platform' as verification_platform,
 				      document#'platform' as platform,
 				      document#'experimental_protocol' as experimental_protocol,
 				      document#'raw_data_repository' as raw_data_repository,
 				      document#'raw_data_accession' as raw_data_accession;
