%default LIB 'udf/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
REGISTER $LIB

%default DATATYPE 'sample'
%default UPLOAD_TO_RELEASE '';

%default OBSERVATION '<observation_files>';

%default TMP_DYNAMIC_DIR   '<tmp_dynamic_dir>'
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

-- import
DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');

%default EMPTY_VALUE '';
%declare EMPTY_SPECIMEN ['_specimen_id'#'$EMPTY_VALUE','specimen_id'#'$EMPTY_VALUE']
%declare EMPTY_SAMPLE ['_sample_id'#'$EMPTY_VALUE','analyzed_sample_id'#'$EMPTY_VALUE','analyzed_sample_interval'#'$EMPTY_VALUE']

set job.name dynamic-$DATATYPE;

import 'projection.pig';

keys = FOREACH (ORDER (GROUP selected_donor BY donor_id) BY group) {
          specimens = FOREACH selected_donor 
                    GENERATE icgc_donor_id..submitted_donor_id,
                             FLATTEN(((specimens is null or IsEmpty(specimens)) ? {($EMPTY_SPECIMEN)} : specimens)) as specimen;
          
          content = FOREACH specimens 
                    GENERATE icgc_donor_id..submitted_donor_id,
                             specimen#'_specimen_id' as icgc_specimen_id, 
                             specimen#'specimen_id' as submitted_specimen_id, 
                             FLATTEN((bag{tuple(map[])}) specimen#'sample') as s;
          
          selected_content = FOREACH content 
                       GENERATE s#'_sample_id' as icgc_sample_id,
                                project_code,
                                s#'analyzed_sample_id' as submitted_sample_id,
                                icgc_specimen_id, 
                                submitted_specimen_id,
                                icgc_donor_id,
                                submitted_donor_id,
                                s#'analyzed_sample_interval' as analyzed_sample_interval,
                                s#'percentage_cellularity' as percentage_cellularity,
                                s#'level_of_cellularity' as level_of_cellularity,
                                s#'study' as study;

              GENERATE FLATTEN(TOHFILE(group, selected_content)) as key;
};

STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();
