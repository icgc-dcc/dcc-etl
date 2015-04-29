%default OBSERVATION '<observation_dir>';
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();
%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader'

%default EMPTY_VALUE '';
%declare EMPTY_THERAPY ['first_therapy_type'#'$EMPTY_VALUE','first_therapy_therapeutic_intent'#'$EMPTY_VALUE','first_therapy_start_interval'#'$EMPTY_VALUE','first_therapy_duration'#'$EMPTY_VALUE','first_therapy_response'#'$EMPTY_VALUE','second_therapy_type'#'$EMPTY_VALUE','second_therapy_therapeutic_intent'#'$EMPTY_VALUE','second_therapy_start_interval'#'$EMPTY_VALUE','second_therapy_duration'#'$EMPTY_VALUE','second_therapy_response'#'$EMPTY_VALUE','other_therapy'#'$EMPTY_VALUE','other_therapy_response'#'$EMPTY_VALUE']

-- load donor 
donor = LOAD '$OBSERVATION' USING $JSON_LOADER('-nestedLoad') as document:map[];

selected_donor = foreach donor generate ExtractId(document#'_donor_id') as donor_id:int,
					                    document#'_donor_id' as icgc_donor_id,
                                        document#'_project_id' as project_code,
                                        document#'donor_id' as submitted_donor_id,
                    (bag{tuple(map[])}) document#'therapy' as therapies;