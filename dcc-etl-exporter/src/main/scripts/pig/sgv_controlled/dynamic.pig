%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'sgv_controlled'
set job.name dynamic-$DATATYPE;

%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'
%default TMP_HFILE_DIR     '/tmp/download/tmp/dynamic/hfile'

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.TOHFILE('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['consequence_type'#'$EMPTY_VALUE','aa_change'#'$EMPTY_VALUE','cds_change'#'$EMPTY_VALUE','gene_affected'#'$EMPTY_VALUE','transcript_affected'#'$EMPTY_VALUE']

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';

keys = foreach (ORDER (GROUP selected_sgv BY donor_id) BY group) {
                    content = FOREACH selected_sgv GENERATE icgc_variant_id..verification_platform, 
                                                             FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence,
                                                             platform..note;
            
           selected_content = FOREACH content GENERATE icgc_variant_id..verification_platform, 
                                                       consequence#'consequence_type' as consequence_type,
                                                       consequence#'aa_change' as aa_change,
                                                       consequence#'cds_change' as cds_change,
                                                       consequence#'gene_affected' as gene_affected,
                                                       consequence#'transcript_affected' as transcript_affected,
                                                       platform..note;
              generate FLATTEN(TOHFILE(group, selected_content)) as key;
}
STORE keys INTO '$TMP_DYNAMIC_DIR' USING com.twitter.elephantbird.pig.store.LzoRawBytesStorage();
