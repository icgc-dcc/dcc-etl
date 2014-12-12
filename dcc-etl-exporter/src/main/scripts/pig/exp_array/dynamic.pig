%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

-- import

%default DATATYPE 'exp_array'

set job.name dynamic-$DATATYPE;
-- set mapred.child.java.opts '-Xmx4096m'
-- set mapred.map.tasks.speculative.execution false
-- set mapred.reduce.tasks.speculative.execution" false
-- set mapred.task.timeout 1800000

%default UPLOAD_TO_RELEASE '';
%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'
%default TMP_HFILE_DIR     '/tmp/download/tmp/dynamic/hfile'

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.TOHFILE('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR','true');

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';

keys = foreach (order (group selected_exp by donor_id) by group) {
                selected_content = FOREACH selected_exp GENERATE icgc_donor_id..reference_sample_type;
       generate FLATTEN(TOHFILE(group, selected_content)) as key;
};

STORE keys INTO '$TMP_DYNAMIC_DIR' USING com.twitter.elephantbird.pig.store.LzoRawBytesStorage();
