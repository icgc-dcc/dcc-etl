%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'mirna'

%default TMP_STATIC_DIR    '/tmp/download/tmp/mirna_static'

set job.name static-$DATATYPE;

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';

static_out = FOREACH selected_mirna 
             GENERATE icgc_donor_id..raw_data_accession; 
            
STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'mirna_seq', 'project_code', 'gz', '\\t');
