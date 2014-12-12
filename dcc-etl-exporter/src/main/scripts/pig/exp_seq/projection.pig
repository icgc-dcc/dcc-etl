%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';
%default OBSERVATION '/icgc/etl/r-53049_0-965_0_all/exp.json/part-*';
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

-- load observation
observation = LOAD '$OBSERVATION' USING  $JSON_LOADER('-nestedLoad') as document:map[];

exp = FILTER observation BY document#'_type' == 'exp_seq';
selected_exp = foreach exp generate 
                        ExtractId(document#'_donor_id') as donor_id:int,
                        document#'_donor_id' as icgc_donor_id,
                        document#'_project_id' as project_code,
                        document#'_specimen_id' as icgc_specimen_id,
                        document#'_sample_id' as icgc_sample_id,
                        document#'analyzed_sample_id' as submitted_sample_id, 
                        document#'analysis_id' as analysis_id,
                        document#'gene_model' as gene_model,
                        document#'gene_id' as gene_id,
                        document#'normalized_read_count' as normalized_read_count,
                        document#'raw_read_count' as raw_read_count,
                        document#'fold_change' as fold_change,
                        document#'assembly_version' as assembly_version,
                        document#'platform' as platform,
                        document#'total_read_count' as total_read_count,
                        document#'experimental_protocol' as experimental_protocol,
                        document#'alignment_algorithm' as alignment_algorithm,
                        document#'normalization_algorithm' as normalization_algorithm,
                        document#'other_analysis_algorithm' as other_analysis_algorithm,
                        document#'sequencing_strategy' as sequencing_strategy,
                        document#'raw_data_repository' as raw_data_repository,
                        document#'raw_data_accession' as raw_data_accession,
                        document#'reference_sample_type' as reference_sample_type;
