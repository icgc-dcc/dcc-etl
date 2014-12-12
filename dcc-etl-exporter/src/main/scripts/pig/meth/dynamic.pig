%default LIB 'udf/dcc-etl-exporter.jar'
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

%default EMPTY_VALUE '';
%default UPLOAD_TO_RELEASE '';
%default TMP_HFILE_DIR     '/tmp/download/tmp/dynamic/hfile'

DEFINE TOHFILE org.icgc.dcc.etl.exporter.pig.udf.TOHFILE('$DATATYPE', '$UPLOAD_TO_RELEASE', '$TMP_HFILE_DIR', 'true');
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();

%declare EMPTY_CONSEQUENCE ['gene_affected'#'$EMPTY_VALUE','gene_build_version'#'$EMPTY_VALUE']

%default TMP_DYNAMIC_DIR   '/tmp/download/tmp/dynamic/$DATATYPE'

set job.name dynamic-$DATATYPE;

import 'projection.pig';

flatten_meth = foreach selected_meth generate donor_id, icgc_donor_id..verification_platform,
					      FLATTEN(((consequences is null or IsEmpty(consequences)) ? {($EMPTY_CONSEQUENCE)} : consequences)) as consequence,
					      platform..raw_data_accession;

selected_content = FOREACH flatten_meth GENERATE donor_id, icgc_donor_id..verification_platform,
                                            consequence#'gene_affected' as gene_affected,
                                            consequence#'gene_build_version' as gene_build_version,
                                            platform..raw_data_accession;

keys = FOREACH (GROUP selected_content BY donor_id) {
	      content = FOREACH selected_content GENERATE icgc_donor_id .. raw_data_accession;
              generate FLATTEN(TOHFILE(group, content)) as key;
};
STORE keys INTO '$TMP_DYNAMIC_DIR' USING com.twitter.elephantbird.pig.store.LzoRawBytesStorage();
