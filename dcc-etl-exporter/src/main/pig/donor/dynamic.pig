%default LIB 'udf/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
REGISTER $LIB

%default DATATYPE 'donor'
-- import

%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '<dynamic_dir>'
%default TMP_HFILE_DIR     '<hfile_dir>'
DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

set job.name dynamic-$DATATYPE;
-- load donor 
import 'projection.pig';

keys = FOREACH (GROUP selected_donor BY donor_id) {
	                   selected_content = FOREACH selected_donor GENERATE icgc_donor_id..cancer_history_first_degree_relative;
             GENERATE FLATTEN(TOHFILE(group, selected_content)) as key;
}

STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();
