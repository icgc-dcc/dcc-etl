%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

set io.sort.mb 1024
set io.sort.factor 100
set io.file.buffer.size 131072

%default DATATYPE 'ssm_controlled'
set job.name dynamic-$DATATYPE;

%default RELEASE_OUT 'dev';
%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '<tmp_dynamic_dir>'
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'

-- import
%default DYNAMIC_BLOCK_SIZE '1024';
%default COMPRESSION_ENABLED 'true'

DEFINE COMPUTE_SIZE org.icgc.dcc.etl.exporter.pig.udf.ComputeSize();
DEFINE COMPOSITE_KEY org.icgc.dcc.etl.exporter.pig.udf.CompositeKey('$DATATYPE', '$UPLOAD_TO_RELEASE', '500000');
DEFINE DYNAMIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.DynamicStorage('$DATATYPE', '$UPLOAD_TO_RELEASE', '$COMPRESSION_ENABLED');
DEFINE STATISTIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.StatisticStorage('$DATATYPE', '$UPLOAD_TO_RELEASE');

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['consequence_type'#'$EMPTY_VALUE','aa_mutation'#'$EMPTY_VALUE','cds_mutation'#'$EMPTY_VALUE','gene_affected'#'$EMPTY_VALUE','transcript_affected'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

import 'projection.pig';

flatten_selected_ssm = FOREACH selected_ssm GENERATE donor_id..biological_validation_platform,
                                                     FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence,
                                                     platform..initial_data_release_date;

filtered_ssm = FOREACH flatten_selected_ssm GENERATE donor_id..biological_validation_platform,
                                        consequence#'consequence_type' as consequence_type,
                                        consequence#'aa_mutation' as aa_mutation,
                                        consequence#'cds_mutation' as cds_mutation,
                                        consequence#'gene_affected' as gene_affected,
                                        consequence#'transcript_affected' as transcript_affected,
                                        consequence#'gene_build_version' as gene_build_version,
                                        platform..initial_data_release_date;

selected_content = FOREACH filtered_ssm GENERATE donor_id,
                                                 COMPOSITE_KEY(donor_id) as rowkey:bytearray,
                                                 icgc_mutation_id..initial_data_release_date;
obj = FOREACH selected_content GENERATE rowkey, {(icgc_mutation_id..initial_data_release_date)};
content = ORDER obj BY rowkey ASC PARALLEL $DEFAULT_PARALLEL;
STORE content INTO '$TMP_HFILE_DIR' USING DYNAMIC_STORAGE();

-- populate statistics
row_size = FOREACH selected_content GENERATE donor_id, COMPUTE_SIZE(icgc_mutation_id..initial_data_release_date) as bytes:long;
row_size_per_donor = GROUP row_size BY donor_id;
stats = FOREACH row_size_per_donor GENERATE group as donor_id, COUNT(row_size.donor_id) as total_line, SUM(row_size.bytes) as total_size;
STORE stats INTO '$TMP_DYNAMIC_DIR' USING STATISTIC_STORAGE();