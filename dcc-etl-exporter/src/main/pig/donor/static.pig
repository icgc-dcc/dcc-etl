%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'donor'
%default STATIC_FILE_NAME_PREFIX '<from-param>'

%default RELEASE_OUT '<release>';

%default TMP_STATIC_DIR    '<static_dir>'
%default OUT_STATIC_DIR    '<dynamic_dir>'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

set job.name static-$DATATYPE;
import 'projection.pig';

static_out = FOREACH selected_donor 
             GENERATE icgc_donor_id..cancer_history_first_degree_relative; 

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage('$TMP_STATIC_DIR', '$STATIC_FILE_NAME_PREFIX', 'project_code', 'gz', '\\t');