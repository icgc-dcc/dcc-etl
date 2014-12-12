%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'exp'

set job.name static-$DATATYPE;
-- set mapred.child.java.opts '-Xmx4096m'
-- set mapred.map.tasks.speculative.execution false
-- set mapred.reduce.tasks.speculative.execution" false
-- set mapred.task.timeout 1800000

%default TMP_STATIC_DIR    '/tmp/download/tmp/exp_static'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';

static_out = foreach selected_exp generate icgc_donor_id..raw_data_accession;
STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'gene_expression', 'project_code', 'gz', '\\t');
