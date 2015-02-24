%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

-- import

%default DATATYPE 'exp_seq'

set job.name dynamic-$DATATYPE;

%default UPLOAD_TO_RELEASE '';
%default TMP_DYNAMIC_DIR   '<tmp_dynamic_dir>'
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR','true');

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';

keys = foreach (order (group selected_exp by donor_id) by group) {
                selected_content = FOREACH selected_exp GENERATE icgc_donor_id..reference_sample_type;
       generate FLATTEN(TOHFILE(group, selected_content)) as key;
};

STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();
