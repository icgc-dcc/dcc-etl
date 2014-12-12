%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB

--set mapred.reduce.parallel.copies 2
set io.sort.mb 1024
set io.sort.factor 100
--set mapred.inmem.merge.threshold 0
--set mapred.job.reduce.input.buffer.percent 1.0
set io.file.buffer.size 131072

%default DEFAULT_PARALLEL '12';
set default_parallel $DEFAULT_PARALLEL;

%default DATATYPE 'meth'
set job.name static-$DATATYPE;

%default EMPTY_VALUE '';
%declare EMPTY_CONSEQUENCE ['gene_affected'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

%default TMP_STATIC_DIR    '/tmp/download/tmp/meth_static'

import 'projection.pig';
flatten_meth = foreach selected_meth generate icgc_donor_id..verification_platform,
					      FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence,
					      platform..raw_data_accession;

selected_content = FOREACH flatten_meth GENERATE icgc_donor_id..verification_platform,
                                            consequence#'gene_affected' as gene_affected,
                                            consequence#'gene_build_version' as gene_build_version,
                                            platform..raw_data_accession;

static_out = FOREACH selected_content GENERATE icgc_donor_id .. raw_data_accession;

STORE static_out INTO '$TMP_STATIC_DIR' USING org.icgc.dcc.etl.exporter.pig.udf.StaticMultiStorage('$TMP_STATIC_DIR', 'methylation', 'project_code', 'gz', '\\t');
