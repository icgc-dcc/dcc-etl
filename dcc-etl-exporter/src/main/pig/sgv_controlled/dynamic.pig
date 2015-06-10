%default LIB 'lib/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
REGISTER $LIB

%default DATATYPE 'sgv_controlled'
set job.name dynamic-$DATATYPE;

%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '<tmp_dynamic_dir>'
%default TMP_HFILE_DIR     '<tmp_hfile_dir>'
%default COMPRESSION_ENABLED 'true'

DEFINE COMPUTE_SIZE org.icgc.dcc.etl.exporter.pig.udf.ComputeSize();
DEFINE COMPOSITE_KEY org.icgc.dcc.etl.exporter.pig.udf.CompositeKey('$DATATYPE', '$UPLOAD_TO_RELEASE', '500000');
DEFINE DYNAMIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.DynamicStorage('$DATATYPE', '$UPLOAD_TO_RELEASE', '$COMPRESSION_ENABLED');
DEFINE STATISTIC_STORAGE org.icgc.dcc.etl.exporter.pig.storage.StatisticStorage('$DATATYPE', '$UPLOAD_TO_RELEASE');

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['consequence_type'#'$EMPTY_VALUE','aa_change'#'$EMPTY_VALUE','cds_change'#'$EMPTY_VALUE','gene_affected'#'$EMPTY_VALUE','transcript_affected'#'$EMPTY_VALUE']

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;
set pig.exec.nocombiner true

import 'projection.pig';

-- create hfiles
flat_sgv = FOREACH selected_sgv GENERATE donor_id,
                                    icgc_donor_id..verification_platform,
                                    FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence,
                                    platform..note;

selected_content = FOREACH flat_sgv GENERATE donor_id, COMPOSITE_KEY(donor_id) as rowkey:bytearray, icgc_donor_id..verification_platform, consequence#'consequence_type' as consequence_type, consequence#'aa_change' as aa_change, consequence#'cds_change' as cds_change, consequence#'gene_affected' as gene_affected, consequence#'transcript_affected' as transcript_affected, platform..note;

obj = FOREACH selected_content GENERATE rowkey, {(icgc_donor_id..note)};

content = ORDER obj BY rowkey ASC PARALLEL $DEFAULT_PARALLEL;
STORE content INTO '$TMP_HFILE_DIR' USING DYNAMIC_STORAGE();

-- populate statistics
row_size = FOREACH selected_content GENERATE donor_id, COMPUTE_SIZE(icgc_donor_id..note) as bytes:long;
row_size_per_donor = GROUP row_size BY donor_id;
stats = FOREACH row_size_per_donor GENERATE group as donor_id, COUNT(row_size.donor_id) as total_line, SUM(row_size.bytes) as total_size;
STORE stats INTO '$TMP_DYNAMIC_DIR' USING STATISTIC_STORAGE();