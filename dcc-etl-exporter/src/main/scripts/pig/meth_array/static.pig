%default LIB 'lib/dcc-etl-exporter.jar'
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
set job.name static-$DATATYPE;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['gene_affected'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

%default TMP_STATIC_DIR    '/tmp/download/tmp/meth_static'

import 'projection.pig';
static_out = FOREACH selected_meth GENERATE icgc_donor_id .. raw_data_accession;
STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'meth_array', 'project_code', 'gz', '\\t');
