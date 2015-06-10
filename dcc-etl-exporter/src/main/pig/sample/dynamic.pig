%default LIB 'udf/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'sample'
%default UPLOAD_TO_RELEASE '';

%default OBSERVATION '<observation_files>';

%default TMP_DYNAMIC_DIR   '<tmp_dynamic_dir>'
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;
%default COMPRESSION_ENABLED 'true'

DEFINE COMPUTE_SIZE org.icgc.dcc.etl.exporter.pig.udf.ComputeSize();
DEFINE COMPOSITE_KEY org.icgc.dcc.etl.exporter.pig.udf.CompositeKey('$DATATYPE', '$UPLOAD_TO_RELEASE', '500000');
DEFINE DYNAMIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.DynamicStorage('$DATATYPE', '$UPLOAD_TO_RELEASE', '$COMPRESSION_ENABLED');
DEFINE STATISTIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.StatisticStorage('$DATATYPE', '$UPLOAD_TO_RELEASE');

%default EMPTY_VALUE '';
%declare EMPTY_SPECIMEN ['_specimen_id'#'$EMPTY_VALUE','specimen_id'#'$EMPTY_VALUE']
%declare EMPTY_SAMPLE ['_sample_id'#'$EMPTY_VALUE','analyzed_sample_id'#'$EMPTY_VALUE','analyzed_sample_interval'#'$EMPTY_VALUE']

set job.name dynamic-$DATATYPE;

import 'projection.pig';

flat_donor = FOREACH selected_donor GENERATE donor_id,
                                            icgc_donor_id..submitted_donor_id,
                                            FLATTEN(((specimens is null or IsEmpty(specimens)) ? {($EMPTY_SPECIMEN)} : specimens)) as specimen;

flat_flat_donor = FOREACH flat_donor GENERATE donor_id,
                                              icgc_donor_id..submitted_donor_id,
                                              specimen#'_specimen_id' as icgc_specimen_id,
                                              specimen#'specimen_id' as submitted_specimen_id,
                                              FLATTEN((bag{tuple(map[])}) specimen#'sample') as s;

selected_content = FOREACH flat_flat_donor GENERATE donor_id,
                                                    COMPOSITE_KEY(donor_id) as rowkey:bytearray,
                                                    s#'_sample_id' as icgc_sample_id,
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

obj = FOREACH selected_content GENERATE rowkey, {(icgc_sample_id..study)};
content = ORDER obj BY rowkey ASC PARALLEL $DEFAULT_PARALLEL;
STORE content INTO '$TMP_HFILE_DIR' USING DYNAMIC_STORAGE();

-- populate statistics
row_size = FOREACH selected_content GENERATE donor_id, COMPUTE_SIZE(icgc_sample_id..study) as bytes:long;
row_size_per_donor = GROUP row_size BY donor_id;
stats = FOREACH row_size_per_donor GENERATE group as donor_id, COUNT(row_size.donor_id) as total_line, SUM(row_size.bytes) as total_size;
STORE stats INTO '$TMP_DYNAMIC_DIR' USING STATISTIC_STORAGE();