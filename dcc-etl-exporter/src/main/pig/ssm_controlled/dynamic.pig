%default LIB 'lib/dcc-etl-exporter.jar'
%default RAW_STORAGE 'com.twitter.elephantbird.pig.store.LzoRawBytesStorage'
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
DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.ToHFile('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR','true');
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId()

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['consequence_type'#'$EMPTY_VALUE','aa_mutation'#'$EMPTY_VALUE','cds_mutation'#'$EMPTY_VALUE','gene_affected'#'$EMPTY_VALUE','transcript_affected'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

import 'projection.pig';

flatten_selected_ssm = FOREACH selected_ssm
          GENERATE donor_id..biological_validation_platform,
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

keys = foreach (GROUP filtered_ssm BY (donor_id)) {
              content = FOREACH filtered_ssm GENERATE icgc_mutation_id..initial_data_release_date;
              generate FLATTEN(TOHFILE(group, content)) as key;
};

STORE keys INTO '$TMP_DYNAMIC_DIR' USING $RAW_STORAGE();