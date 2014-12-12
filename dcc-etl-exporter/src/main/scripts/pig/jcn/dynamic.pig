%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'jcn'

%default EMPTY_VALUE '';
%default UPLOAD_TO_RELEASE '';
%default DEFAULT_PARALLEL '3';
%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'
%default TMP_HFILE_DIR     '/tmp/download/tmp/dynamic/hfile'

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.TOHFILE('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

set job.name dynamic-$DATATYPE;
set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';
keys = foreach (ORDER (GROUP selected_jcn BY donor_id) by group) {
            selected_content = FOREACH selected_jcn GENERATE icgc_donor_id..raw_data_accession;
              generate FLATTEN(TOHFILE(group, selected_content)) as key;
}
STORE keys INTO '$TMP_DYNAMIC_DIR' USING com.twitter.elephantbird.pig.store.LzoRawBytesStorage();
