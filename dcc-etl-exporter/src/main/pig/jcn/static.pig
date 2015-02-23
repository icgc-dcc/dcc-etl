%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB
%default DATATYPE 'jcn'

%default EMPTY_VALUE '';
%default RELEASE_OUT 'dev';
%default TMP_STATIC_DIR    '<tmp_static_dir>'
%default DEFAULT_PARALLEL '3';
%default OUT_STATIC_DIR    '<out_static_dir>'

set job.name static-$DATATYPE;
set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';
static_out = FOREACH selected_jcn 
             GENERATE icgc_donor_id..raw_data_accession;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage('$TMP_STATIC_DIR', 'splice_variant', 'project_code', 'gz', '\\t');
