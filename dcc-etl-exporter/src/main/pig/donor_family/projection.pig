%default OBSERVATION '<observation_dir>';
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();
%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader'

%default EMPTY_VALUE '';
%declare EMPTY_FAMILY ['donor_has_relative_with_cancer_history'#'$EMPTY_VALUE','relationship_type'#'$EMPTY_VALUE','relationship_type_other'#'$EMPTY_VALUE','relationship_sex'#'$EMPTY_VALUE','relationship_age'#'$EMPTY_VALUE','relationship_disease_icd10'#'$EMPTY_VALUE','relationship_disease'#'$EMPTY_VALUE']

-- load donor 
donor = LOAD '$OBSERVATION' USING $JSON_LOADER('-nestedLoad') as document:map[];

selected_donor = foreach donor generate ExtractId(document#'_donor_id') as donor_id:int,
					document#'_donor_id' as icgc_donor_id,
                                        document#'_project_id' as project_code,
                                        document#'donor_id' as submitted_donor_id,
                    (bag{tuple(map[])}) document#'family' as families;