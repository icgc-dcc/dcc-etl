%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'mirna'

%default EMPTY_VALUE '';
%default UPLOAD_TO_RELEASE '';
%default DEFAULT_PARALLEL '3';
%default TMP_HFILE_DIR     '/tmp/download/tmp/dynamic/hfile'

set job.name dynamic-$DATATYPE;
set default_parallel $DEFAULT_PARALLEL;

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.TOHFILE('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR','true');

%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'

%declare EMPTY_CONSEQUENCE ['chromosome'#'$EMPTY_VALUE','chromosome_start'#'$EMPTY_VALUE','chromosome_end'#'$EMPTY_VALUE','chromosome_strand'#'$EMPTY_VALUE','xref_mirbase_id'#'$EMPTY_VALUE']

import 'projection.pig';
keys = foreach (ORDER (GROUP selected_mirna BY donor_id) BY group) {
                     content = FOREACH selected_mirna GENERATE icgc_donor_id..verification_platform, 
                                                       FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence, 
                                                       assembly_version..raw_data_accession;
            
            selected_content = FOREACH content GENERATE icgc_donor_id..verification_platform, 
                                                        consequence#'chromosome' as chromosome,
                                                        consequence#'chromosome_start' as chromosome_start,
                                                        consequence#'chromosome_end' as chromosome_end,
                                                        consequence#'chromosome_strand' as chromosome_strand,
                                                        assembly_version,
                                                        consequence#'xref_mirbase_id' as xref_mirbase_id,
                                                        gene_build_version..raw_data_accession;
                         -- key = (group.project_code, group.icgc_donor_id, 'tsv', '$DATATYPE');
              -- generate FLATTEN(CreateIndex(key, selected_content));
              generate FLATTEN(TOHFILE(group, selected_content)) as key;
}
STORE keys INTO '$TMP_DYNAMIC_DIR' USING com.twitter.elephantbird.pig.store.LzoRawBytesStorage();
