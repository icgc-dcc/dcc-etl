%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'exp_seq'

set job.name static-$DATATYPE;

%default TMP_STATIC_DIR    '<tmp_static_dir>'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';

static_out = foreach selected_exp generate icgc_donor_id..reference_sample_type;
STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage('$TMP_STATIC_DIR', 'exp_seq', 'project_code', 'gz', '\\t');
