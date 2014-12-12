%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'clinicalsample'
%default UPLOAD_TO_RELEASE '';

%default OBSERVATION '/icgc/etl/r-53049_0-965_0_all/donor.json/part-*';

%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'
%default TMP_HFILE_DIR     '/tmp/download/tmp/dynamic/hfile'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

-- import
DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.TOHFILE('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default EMPTY_VALUE '';
%declare EMPTY_SPECIMEN ['_specimen_id'#'$EMPTY_VALUE','specimen_id'#'$EMPTY_VALUE']
%declare EMPTY_SAMPLE ['_sample_id'#'$EMPTY_VALUE','analyzed_sample_id'#'$EMPTY_VALUE','analyzed_sample_interval'#'$EMPTY_VALUE']

set job.name dynamic-$DATATYPE;

import 'projection.pig';

keys = FOREACH (ORDER (GROUP selected_donor BY donor_id) BY group) {
                  specimens = FOREACH selected_donor GENERATE project_code,
                                                              icgc_donor_id,
                                                              FLATTEN(((specimens is null or IsEmpty(specimens)) ? {($EMPTY_SPECIMEN)} : specimens)) as specimen;

                    content = FOREACH specimens GENERATE project_code, 
                                                         specimen#'_specimen_id' as icgc_specimen_id, 
                                                         icgc_donor_id,
                                                         specimen#'specimen_id' as submitted_specimen_id, 
     	                             FLATTEN((bag{tuple(map[])}) specimen#'sample') as s;

                    selected_content =  FOREACH content GENERATE s#'_sample_id' as icgc_sample_id,
                                                          project_code,
                                                          icgc_specimen_id, 
                                                          icgc_donor_id,
                                                          s#'analyzed_sample_id' as submitted_sample_id,
                                                          submitted_specimen_id, 
                                                          s#'analyzed_sample_interval' as analyzed_sample_interval;

              GENERATE FLATTEN(TOHFILE(group, selected_content)) as key;
};

STORE keys INTO '$TMP_DYNAMIC_DIR' USING com.twitter.elephantbird.pig.store.LzoRawBytesStorage();
