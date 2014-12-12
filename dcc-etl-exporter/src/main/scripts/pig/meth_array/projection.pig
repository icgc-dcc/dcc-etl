%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';
%default OBSERVATION '/icgc/etl/r-22-projects2_all/meth.json/part-*';

DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

-- load observation
observation = LOAD '$OBSERVATION' USING $JSON_LOADER('-nestedLoad') as document:map[];

meth = FILTER observation BY document#'_type' == '$DATATYPE';
selected_meth = foreach meth generate ExtractId(document#'_donor_id') as donor_id:int,
				      document#'_donor_id' as icgc_donor_id,
                                      document#'_project_id' as project_code,
                                      document#'_specimen_id' as icgc_specimen_id,
                                      document#'_sample_id' as icgc_sample_id,
                                      document#'analyzed_sample_id' as submitted_sample_id, 
                                      document#'analysis_id' as analysis_id,
                                      document#'array_platform' as array_platform,
                                      document#'probe_id' as probe_id,
                                      document#'methylation_value' as methylation_value,
                                      document#'metric_used' as metric_used,
                                      document#'methylated_probe_intensity' as methylated_probe_intensity,
                                      document#'unmethylated_probe_intensity' as unmethylated_probe_intensity,
                                      document#'verification_status' as verification_status,
                                      document#'verification_platform' as verification_platform,
                                      document#'fraction_wg_cpg_sites_covered' as fraction_wg_cpg_sites_covered,
                                      document#'conversion_rate' as conversion_rate,
                                      document#'experimental_protocol' as experimental_protocol,
                                      document#'other_analysis_algorithm' as other_analysis_algorithm,
                                      document#'raw_data_repository' as raw_data_repository,
                                      document#'raw_data_accession' as raw_data_accession;
