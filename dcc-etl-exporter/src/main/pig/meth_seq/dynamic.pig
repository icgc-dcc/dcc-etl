%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'

%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

set io.sort.mb 1024
set io.sort.factor 100
set io.file.buffer.size 131072

%default DEFAULT_PARALLEL '12';
set default_parallel $DEFAULT_PARALLEL;

%default DATATYPE 'meth_seq'

%default UPLOAD_TO_RELEASE '';
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default TMP_DYNAMIC_DIR   '<tmp_dynamic_dir>'

set job.name dynamic-$DATATYPE;

import 'projection.pig';

keys = FOREACH (GROUP selected_meth BY donor_id) {
	      content = FOREACH selected_meth GENERATE icgc_donor_id .. raw_data_accession;
              generate FLATTEN(TOHFILE(group, content)) as key;
};
STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();