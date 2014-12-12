%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'dynamic_ssm_open'
set job.name dynamic-$DATATYPE;

%default UPLOAD_TO_RELEASE '';

%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'
%default TMP_HFILE_DIR     '/tmp/download/tmp/dynamic/hfile'

%default DYNAMIC_BLOCK_SIZE '1024';
DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.TOHFILE('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR','true');
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['consequence_type'#'$EMPTY_VALUE','aa_mutation'#'$EMPTY_VALUE','cds_mutation'#'$EMPTY_VALUE','gene_affected'#'$EMPTY_VALUE','transcript_affected'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

IMPORT 'projection.pig';

keys = foreach (ORDER (GROUP selected_ssm BY (donor_id)) BY group) {
                    content = FOREACH selected_ssm GENERATE icgc_mutation_id..biological_validation_platform, 
                                                             FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence,
                                                             platform..initial_data_release_date;
            
           selected_content = FOREACH content GENERATE icgc_mutation_id..biological_validation_platform,
                                                        consequence#'consequence_type' as consequence_type,
                                                        consequence#'aa_mutation' as aa_mutation,
                                                        consequence#'cds_mutation' as cds_mutation,
                                                        consequence#'gene_affected' as gene_affected,
                                                        consequence#'transcript_affected' as transcript_affected,
                                                        consequence#'gene_build_version' as gene_build_version,
                                                       platform..initial_data_release_date;
              generate FLATTEN(TOHFILE(group, selected_content)) as key;
};

STORE keys INTO '$TMP_DYNAMIC_DIR' USING com.twitter.elephantbird.pig.store.LzoRawBytesStorage();
