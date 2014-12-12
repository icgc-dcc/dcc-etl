%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

--set mapred.reduce.parallel.copies 2
set io.sort.mb 1024
set io.sort.factor 100
--set mapred.inmem.merge.threshold 0
--set mapred.job.reduce.input.buffer.percent 1.0
set io.file.buffer.size 131072

%default DEFAULT_PARALLEL '12';
set default_parallel $DEFAULT_PARALLEL;

%default DATATYPE 'meth_array'

%default UPLOAD_TO_RELEASE '';
%default TMP_HFILE_DIR     '/tmp/download/tmp/dynamic/hfile'

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.TOHFILE('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'

set job.name dynamic-$DATATYPE;

import 'projection.pig';

keys = FOREACH (GROUP selected_meth BY donor_id) {
	      content = FOREACH selected_meth GENERATE icgc_donor_id .. raw_data_accession;
              generate FLATTEN(TOHFILE(group, content)) as key;
};
STORE keys INTO '$TMP_DYNAMIC_DIR' USING com.twitter.elephantbird.pig.store.LzoRawBytesStorage();
