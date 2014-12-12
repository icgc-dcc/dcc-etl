%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'mirna'

%default TMP_STATIC_DIR    '/tmp/download/tmp/mirna_static'

set job.name static-$DATATYPE;

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['chromosome'#'$EMPTY_VALUE','chromosome_start'#'$EMPTY_VALUE','chromosome_end'#'$EMPTY_VALUE','chromosome_strand'#'$EMPTY_VALUE','xref_mirbase_id'#'$EMPTY_VALUE']

import 'projection.pig';

content = FOREACH selected_mirna 
             GENERATE icgc_donor_id..verification_platform, 
                      FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence, 
                      assembly_version..raw_data_accession;
            
static_out = FOREACH content 
             GENERATE icgc_donor_id..verification_platform, 
                      consequence#'chromosome' as chromosome,
                      consequence#'chromosome_start' as chromosome_start,
                      consequence#'chromosome_end' as chromosome_end,
                      consequence#'chromosome_strand' as chromosome_strand,
                      assembly_version,
                      consequence#'xref_mirbase_id' as xref_mirbase_id,
                      gene_build_version..raw_data_accession;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'mirna_expression', 'project_code', 'gz', '\\t');
