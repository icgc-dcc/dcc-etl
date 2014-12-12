%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'cnsm'
set job.name static-$DATATYPE;

%default RELEASE_OUT 'r12';
%default TMP_STATIC_DIR    '/tmp/download/tmp/cnsm_static'

-- import
%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['gene_affected'#'$EMPTY_VALUE','transcript_affected'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

%default OUT_STATIC_DIR    '/tmp/download/static/$RELEASE_OUT'
%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'

import 'projection.pig';

content = FOREACH selected_cnsm 
             GENERATE icgc_donor_id..verification_platform,
                      FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence, 
                      platform..raw_data_accession;
            
static_out = FOREACH content 
             GENERATE icgc_donor_id..verification_platform,
                      consequence#'gene_affected' as gene_affected,
                      consequence#'transcript_affected' as transcript_affected,
                      consequence#'gene_build_version' as gene_build_version,
                      platform..raw_data_accession;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'copy_number_somatic_mutation', 'project_code', 'gz', '\\t');
