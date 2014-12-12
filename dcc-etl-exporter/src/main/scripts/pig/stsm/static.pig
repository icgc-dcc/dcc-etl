%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'stsm'
%default TMP_STATIC_DIR    '/tmp/download/tmp/stsm_static'

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['gene_affected_by_bkpt_from'#'$EMPTY_VALUE','gene_affected_by_bkpt_to'#'$EMPTY_VALUE','transcript_affected_by_bkpt_from'#'$EMPTY_VALUE','transcript_affected_by_bkpt_to'#'$EMPTY_VALUE','bkpt_from_context'#'$EMPTY_VALUE','bkpt_to_context'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

set job.name static-$DATATYPE;

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';
content = FOREACH selected_stsm 
          GENERATE icgc_donor_id..verification_platform,
                   FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence, 
                   platform..raw_data_accession;
            
static_out = FOREACH content 
             GENERATE icgc_donor_id..verification_platform,
                      consequence#'gene_affected_by_bkpt_from' as gene_affected_by_bkpt_from,
                      consequence#'gene_affected_by_bkpt_to' as gene_affected_by_bkpt_to,
                      consequence#'transcript_affected_by_bkpt_from' as transcript_affected_by_bkpt_from,
                      consequence#'transcript_affected_by_bkpt_to' as transcript_affected_by_bkpt_to,
                      consequence#'bkpt_from_context' as bkpt_from_context,
                      consequence#'bkpt_to_context' as bkpt_to_context,
                      consequence#'gene_build_version' as gene_build_version,
                      platform..raw_data_accession;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'structural_somatic_mutation', 'project_code', 'gz', '\\t');
