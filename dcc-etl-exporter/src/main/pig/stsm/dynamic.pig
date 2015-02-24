%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'stsm'
%default UPLOAD_TO_RELEASE '';
%default DEFAULT_PARALLEL '3';
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR','true');

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['gene_affected_by_bkpt_from'#'$EMPTY_VALUE','gene_affected_by_bkpt_to'#'$EMPTY_VALUE','transcript_affected_by_bkpt_from'#'$EMPTY_VALUE','transcript_affected_by_bkpt_to'#'$EMPTY_VALUE','bkpt_from_context'#'$EMPTY_VALUE','bkpt_to_context'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

set job.name dynamic-$DATATYPE;
set default_parallel $DEFAULT_PARALLEL;

%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'

import 'projection.pig';
-- Dynamic --
keys = foreach (ORDER (GROUP selected_stsm BY donor_id) BY group) {
                     content = FOREACH selected_stsm GENERATE icgc_donor_id..verification_platform,
                                                              FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence, 
                                                              platform..raw_data_accession;
            
            selected_content = FOREACH content GENERATE icgc_donor_id..verification_platform,
                                                        consequence#'gene_affected_by_bkpt_from' as gene_affected_by_bkpt_from,
                                                        consequence#'gene_affected_by_bkpt_to' as gene_affected_by_bkpt_to,
                                                        consequence#'transcript_affected_by_bkpt_from' as transcript_affected_by_bkpt_from,
                                                        consequence#'transcript_affected_by_bkpt_to' as transcript_affected_by_bkpt_to,
                                                        consequence#'bkpt_from_context' as bkpt_from_context,
                                                        consequence#'bkpt_to_context' as bkpt_to_context,
                                                        consequence#'gene_build_version' as gene_build_version,
                                                        platform..raw_data_accession;
                         -- key = (group.project_code, group.icgc_donor_id, 'tsv', '$DATATYPE');
              -- generate FLATTEN(CreateIndex(key, selected_content));
              generate FLATTEN(TOHFILE(group, selected_content)) as key;
}
STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();
