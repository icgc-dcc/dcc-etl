%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'pexp'

set job.name static-$DATATYPE;

%default EMPTY_VALUE       '';
%default RELEASE_OUT       'dev';
%default TMP_STATIC_DIR    '/tmp/download/tmp/pexp_static'
%default DEFAULT_PARALLEL '3';

set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';
static_out = FOREACH selected_pexp
             GENERATE icgc_donor_id..raw_data_accession;
STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'protein_expression', 'project_code', 'gz', '\\t');
