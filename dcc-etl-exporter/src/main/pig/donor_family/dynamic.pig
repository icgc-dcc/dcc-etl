%default LIB 'udf/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
REGISTER $LIB

%default DATATYPE 'donor_family'
-- import

%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '<dynamic_dir>'
%default TMP_HFILE_DIR     '<hfile_dir>'
DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default EMPTY_VALUE '';
%declare EMPTY_FAMILY ['donor_has_relative_with_cancer_history'#'$EMPTY_VALUE','relationship_type'#'$EMPTY_VALUE','relationship_type_other'#'$EMPTY_VALUE','relationship_sex'#'$EMPTY_VALUE','relationship_age'#'$EMPTY_VALUE','relationship_disease_icd10'#'$EMPTY_VALUE','relationship_disease'#'$EMPTY_VALUE']

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

set job.name dynamic-$DATATYPE;
-- load donor 
import 'projection.pig';

keys = foreach (GROUP selected_donor BY donor_id) {
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

             GENERATE FLATTEN(TOHFILE(group, selected_content)) as key;
}
STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();