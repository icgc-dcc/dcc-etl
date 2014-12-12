set job.name 'load-balancing-$TABLE_NAME';
SET pig.noSplitCombination true;
%default LIB 'lib/dcc-etl-exporter.jar'
REGISTER $LIB
%default TABLE_NAME 'test';
%default NUM_REGION '14';
%default KEYSPACE '/tmp/download/tmp/dynamic/meth-debug/methexporter/part-*'
set hbase.zookeeper.quorum '***REMOVED***'
set hbase.zookeeper.property.clientPort '2181'

DEFINE CreateTable org.icgc.dcc.etl.exporter.pig.udf.CreateTable('$TABLE_NAME', '$NUM_REGION');

keys = LOAD '$KEYSPACE' USING com.twitter.elephantbird.pig.load.LzoRawBytesLoader() as (key:bytearray);
filtered_keys = FILTER keys by key is not null;
out = foreach (group filtered_keys ALL) {
	sorted_keys = order filtered_keys by key;
	generate FLATTEN(CreateTable(sorted_keys));
};

store out into '$TMP_BUCKET_DIR';
