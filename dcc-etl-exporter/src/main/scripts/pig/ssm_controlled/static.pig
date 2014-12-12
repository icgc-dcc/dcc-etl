%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'static_ssm_controlled'
set job.name static-$DATATYPE;

%default OBSERVATION '/icgc/etl/r-22-projects2_all/ssm.json/part-*';
%default RELEASE_OUT 'dev';
%default UPLOAD_TO_RELEASE '';

%default TMP_STATIC_DIR    '/tmp/download/tmp/ssm_controlled_static';
%default OUT_STATIC_DIR    '/tmp/download/static/$RELEASE_OUT'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['consequence_type'#'$EMPTY_VALUE','aa_mutation'#'$EMPTY_VALUE','cds_mutation'#'$EMPTY_VALUE','gene_affected'#'$EMPTY_VALUE','transcript_affected'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

import 'projection.pig';

content = FOREACH selected_ssm 
          GENERATE icgc_mutation_id..biological_validation_platform, 
                   FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence, 
                   platform..initial_data_release_date;
            
static_out = FOREACH content 
                   GENERATE icgc_mutation_id..biological_validation_platform,
			    consequence#'consequence_type' as consequence_type,
			    consequence#'aa_mutation' as aa_mutation,
			    consequence#'cds_mutation' as cds_mutation,
			    consequence#'gene_affected' as gene_affected,
			    consequence#'transcript_affected' as transcript_affected,
			    consequence#'gene_build_version' as gene_build_version,
			    platform..initial_data_release_date;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'simple_somatic_mutation.controlled', 'project_code', 'gz', '\\t');
