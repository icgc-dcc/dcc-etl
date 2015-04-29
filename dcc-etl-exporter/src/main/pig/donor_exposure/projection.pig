%default OBSERVATION '<observation_dir>';
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();
%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader'

%default EMPTY_VALUE '';
%declare EMPTY_EXPOSURE ['exposure_type'#'$EMPTY_VALUE','exposure_intensity'#'$EMPTY_VALUE','tobacco_smoking_history_indicator'#'$EMPTY_VALUE','tobacco_smoking_intensity'#'$EMPTY_VALUE','alcohol_history'#'$EMPTY_VALUE','alcohol_history_intensity'#'$EMPTY_VALUE']

-- load donor 
donor = LOAD '$OBSERVATION' USING $JSON_LOADER('-nestedLoad') as document:map[];

selected_donor = foreach donor generate ExtractId(document#'_donor_id') as donor_id:int,
					                    document#'_donor_id' as icgc_donor_id,
                                        document#'_project_id' as project_code,
                                        document#'donor_id' as submitted_donor_id,
                    (bag{tuple(map[])}) document#'exposure' as exposures;