%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'cnsm'
set job.name dynamic-$DATATYPE;

%default UPLOAD_TO_RELEASE '';
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'

-- import
DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['gene_affected'#'$EMPTY_VALUE','transcript_affected'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'

import 'projection.pig';

-- Dynamic --
keys = foreach (ORDER (GROUP selected_cnsm BY (donor_id)) BY group) {
                     content = FOREACH selected_cnsm GENERATE icgc_donor_id..verification_platform,
                                                             FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence, 
                                                             platform..raw_data_accession;
            
            selected_content = FOREACH content GENERATE icgc_donor_id..verification_platform,
                                                        consequence#'gene_affected' as gene_affected,
                                                        consequence#'transcript_affected' as transcript_affected,
                                                        consequence#'gene_build_version' as gene_build_version,
                                                        platform..raw_data_accession;
              generate FLATTEN(TOHFILE(group, selected_content)) as key;
}
STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();
