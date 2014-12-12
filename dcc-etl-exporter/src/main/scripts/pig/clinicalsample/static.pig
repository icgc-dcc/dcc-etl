%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

%default DATATYPE 'clinicalsample'
set job.name static-$DATATYPE;

%default RELEASE_OUT 'r12';
%default TMP_STATIC_DIR    '/tmp/download/tmp/clinicalsample_static'
%default OUT_STATIC_DIR    '/tmp/download/static/$RELEASE_OUT'

%default DEFAULT_PARALLEL '3';
set default_parallel $DEFAULT_PARALLEL;

%default EMPTY_VALUE '';
%declare EMPTY_SPECIMEN ['_specimen_id'#'$EMPTY_VALUE','specimen_id'#'$EMPTY_VALUE']
%declare EMPTY_SAMPLE ['_sample_id'#'$EMPTY_VALUE','analyzed_sample_id'#'$EMPTY_VALUE','analyzed_sample_interval'#'$EMPTY_VALUE']

-- load donor 
import 'projection.pig';

content = FOREACH selected_donor 
          GENERATE project_code, 
                   icgc_donor_id,
                   FLATTEN(((specimens is null or IsEmpty(specimens)) ? {($EMPTY_SPECIMEN)} : specimens)) as specimen;

content = FOREACH content 
          GENERATE project_code, 
                   specimen#'_specimen_id' as icgc_specimen_id, 
                   icgc_donor_id,
                   specimen#'specimen_id' as submitted_specimen_id, 
                   FLATTEN((bag{tuple(map[])}) specimen#'sample') as s;

static_out = FOREACH content 
             GENERATE s#'_sample_id' as icgc_sample_id,
                      project_code,
                      icgc_specimen_id, 
                      icgc_donor_id,
                      s#'analyzed_sample_id' as submitted_sample_id,
                      submitted_specimen_id,
                      s#'analyzed_sample_interval' as analyzed_sample_interval;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'clinicalsample', 'project_code', 'gz', '\\t');
