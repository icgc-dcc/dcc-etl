%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'

%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'mirna'

%default UPLOAD_TO_RELEASE '';
%default DEFAULT_PARALLEL '3';
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'

set job.name dynamic-$DATATYPE;
set default_parallel $DEFAULT_PARALLEL;

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR','true');

%default TMP_DYNAMIC_DIR   '<tmp_dynamic_dir>'

import 'projection.pig';
keys = FOREACH (GROUP selected_mirna BY donor_id) {
                     selected_content = FOREACH selected_mirna GENERATE icgc_donor_id..raw_data_accession; 
       GENERATE FLATTEN(TOHFILE(group, selected_content)) as key;
}
STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();
