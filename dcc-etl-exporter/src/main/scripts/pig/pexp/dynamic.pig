%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'pexp'

set job.name dynamic-$DATATYPE;

%default EMPTY_VALUE       '';
%default UPLOAD_TO_RELEASE '';
%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamtic/$DATATYPE'
%default DEFAULT_PARALLEL '3';
%default TMP_HFILE_DIR     '/tmp/download/tmp/dynamic/hfile'

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.TOHFILE('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR','true');

set default_parallel $DEFAULT_PARALLEL;

import 'projection.pig';
keys = foreach (ORDER (GROUP selected_pexp BY donor_id) BY group) {
            selected_content = FOREACH selected_pexp GENERATE icgc_donor_id..raw_data_accession;
                         -- key = (group.project_code, group.icgc_donor_id, 'tsv', '$DATATYPE');
    -- generate FLATTEN(CreateIndex(key, selected_content));
    generate FLATTEN(TOHFILE(group, selected_content)) as key;
};
STORE keys INTO '$TMP_DYNAMIC_DIR' USING com.twitter.elephantbird.pig.store.LzoRawBytesStorage();
