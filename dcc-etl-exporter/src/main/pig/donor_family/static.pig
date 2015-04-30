%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'donor_family'
%default STATIC_FILE_NAME_PREFIX '<from-param>'

%default RELEASE_OUT '<release>';

%default TMP_STATIC_DIR    '<static_dir>'
%default OUT_STATIC_DIR    '<dynamic_dir>'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_FAMILY ['donor_has_relative_with_cancer_history'#'$EMPTY_VALUE','relationship_type'#'$EMPTY_VALUE','relationship_type_other'#'$EMPTY_VALUE','relationship_sex'#'$EMPTY_VALUE','relationship_age'#'$EMPTY_VALUE','relationship_disease_icd10'#'$EMPTY_VALUE','relationship_disease'#'$EMPTY_VALUE']

set job.name static-$DATATYPE;
import 'projection.pig';

content = FOREACH selected_donor 
             GENERATE icgc_donor_id..submitted_donor_id, 
             FLATTEN(families) as family;

selected_content = FOREACH content GENERATE icgc_donor_id..submitted_donor_id, 
                                      family#'donor_has_relative_with_cancer_history' as donor_has_relative_with_cancer_history,
                                      family#'relationship_type' as relationship_type,
                                      family#'relationship_type_other' as relationship_type_other,
                                      family#'relationship_sex' as relationship_sex,
                                      family#'relationship_age' as relationship_age,
                                      family#'relationship_disease_icd10' as relationship_disease_icd10,
                                      family#'relationship_disease' as relationship_disease;

static_out = ORDER selected_content BY icgc_donor_id;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage('$TMP_STATIC_DIR', '$STATIC_FILE_NAME_PREFIX', 'project_code', 'gz', '\\t');
