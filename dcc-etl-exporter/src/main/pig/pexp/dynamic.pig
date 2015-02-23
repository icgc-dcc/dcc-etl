%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'pexp'

set job.name dynamic-$DATATYPE;

%default EMPTY_VALUE       '';
%default UPLOAD_TO_RELEASE '';
%default TMP_DYNAMIC_DIR   '<tmp_dynamic_dir>'
%default DEFAULT_PARALLEL '3';
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR','true');

set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';
keys = foreach (ORDER (GROUP selected_pexp BY donor_id) BY group) {
            selected_content = FOREACH selected_pexp GENERATE icgc_donor_id..raw_data_accession;
       generate FLATTEN(TOHFILE(group, selected_content)) as key;
};
STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();
