%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'sgv_controlled'
set job.name static-$DATATYPE;

%default TMP_STATIC_DIR    '/tmp/download/tmp/sgv_controlled_static';

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['consequence_type'#'$EMPTY_VALUE','aa_change'#'$EMPTY_VALUE','cds_change'#'$EMPTY_VALUE','gene_affected'#'$EMPTY_VALUE','transcript_affected'#'$EMPTY_VALUE']

import 'projection.pig';

content = FOREACH selected_sgv 
          GENERATE icgc_variant_id..verification_platform, 
                   FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence,
                   platform..note;
            
static_out = FOREACH content 
             GENERATE icgc_variant_id..verification_platform, 
                      consequence#'consequence_type' as consequence_type,
                      consequence#'aa_change' as aa_change,
                      consequence#'cds_change' as cds_change,
                      consequence#'gene_affected' as gene_affected,
                      consequence#'transcript_affected' as transcript_affected,
                      platform..note;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'simple_germline_variation.controlled', 'project_code', 'gz', '\\t');
