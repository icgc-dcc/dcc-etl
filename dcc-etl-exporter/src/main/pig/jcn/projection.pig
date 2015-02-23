%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';
%default OBSERVATION '<observation_files>';

DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

-- load observation
observation = LOAD '$OBSERVATION' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as document:map[];

jcn = FILTER observation BY document#'_type' == '$DATATYPE';
selected_jcn = foreach jcn generate 
                                    ExtractId(document#'_donor_id') as donor_id:int,
				    document#'_donor_id' as icgc_donor_id,
                                    document#'_project_id' as project_code,
                                    document#'_specimen_id' as icgc_specimen_id,
                                    document#'_sample_id' as icgc_sample_id,
				    document#'analyzed_sample_id' as submitted_sample_id, 
                                    document#'analysis_id' as analysis_id,
                                    document#'junction_id' as junction_id,
                                    document#'gene_stable_id' as gene_stable_id,
                                    document#'gene_chromosome' as gene_chromosome,
                                    document#'gene_strand' as gene_strand,
                                    document#'gene_start' as gene_start,
                                    document#'gene_end' as gene_end,
                                    document#'assembly_version' as assembly_version,
                                    document#'second_gene_stable_id' as second_gene_stable_id,
                                    document#'exon1_chromosome' as exon1_chromosome,
                                    document#'exon1_number_bases' as exon1_number_bases,
                                    document#'exon1_end' as exon1_end,
                                    document#'exon1_strand' as exon1_strand,
                                    document#'exon2_chromosome' as exon2_chromosome,
                                    document#'exon2_number_bases' as exon2_number_bases,
                                    document#'exon2_start' as exon2_start,
                                    document#'exon2_strand' as exon2_strand,
                                    document#'is_fusion_gene' as is_fusion_gene,
                                    document#'is_novel_splice_form' as is_novel_splice_form,
                                    document#'junction_seq' as junction_seq,
                                    document#'junction_type' as junction_type,
                                    document#'junction_read_count' as junction_read_count,
                                    document#'quality_score' as quality_score,
                                    document#'probability' as probability,
                                    document#'verification_status' as verification_status,
                                    document#'verification_platform' as verification_platform,
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
