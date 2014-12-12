%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';
%default OBSERVATION '/icgc/etl/r-53049_0-965_0_all/donor.json/part-*';

DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

-- load donor 
donor = LOAD '$OBSERVATION' USING $JSON_LOADER('-nestedLoad') as document:map[];

selected_donor = FOREACH donor GENERATE 
                                        ExtractId(document#'_donor_id') as donor_id:int,
					document#'_project_id' as project_code,
                                        document#'_donor_id' as icgc_donor_id,
                    (bag{tuple(map[])}) document#'specimen' as specimens;
