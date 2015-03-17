import os
from org.apache.pig.scripting import *
from org.apache.hadoop.hbase import HBaseConfiguration

########USER-DEFINED SETTINGS###########
release = "default"
data_source = '/icgc/etl/default'  # this needs to be updated for every new release
upload = None

########CONTROL SETTINGS (INTERNAL) ###########
root = "/tmp/download"
root_out_static = root + "/static"
tmp = root + "/tmp"
tmp_dynamic_root = tmp + '/dynamic'
tmp_hfile_root = tmp + '/hfile'
tmp_static_root = tmp + '/static'
tmp_bucket_root = tmp + '/bucket'
tmp_index = tmp + '/fullindex'
out_dynamic = root + '/dynamic/'
root_validation = root + '/validation'
root_backup = "/nfs/backups/dcc-download-images"

default_exporter_src = '.'

part = '/*/part-*'
# loader = 'com.twitter.elephantbird.pig.load.JsonLoader'
loader = 'com.twitter.elephantbird.pig.load.LzoJsonLoader'

########HADOOP MAPREDUCE ###########
Pig.set("io.sort.mb", "500")
Pig.set("io.sort.factor", "50")
# Pig.set("pig.splitCombination","false")
Pig.set("mapred.map.tasks.speculative.execution", "false")
Pig.set("mapred.reduce.tasks.speculative.execution", "false")
Pig.set("mapreduce.task.classpath.user.precedence", "true")
Pig.set("dfs.client.read.shortcircuit", "true")
Pig.set("io.file.buffer.size", "131072")


######## COMPRESSION ###########
Pig.set("io.compression.codecs","org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.SnappyCodec")
# Map Output #
Pig.set("mapred.compress.map.output", "true")
# Pig.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.GzipCodec")
Pig.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec")

# MapRed intermediate output (Pig control) #
Pig.set("pig.tmpfilecompression", "true")
# Pig.set("pig.tmpfilecompression.codec","gz")
Pig.set("pig.tmpfilecompression.codec", "lzo")

# Actual output (Client) #
#Pig.set("output.compression.enabled", "true")
#Pig.set("output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")
# Pig.set("output.compression.codec","org.apache.hadoop.io.compress.GzipCodec")
# Pig.set("zlib.compress.level", "BEST_SPEED")

######## HBASE ###########
conf = HBaseConfiguration.create()

Pig.set("hbase.zookeeper.quorum", conf.get("hbase.zookeeper.quorum"))
Pig.set("hbase.zookeeper.property.clientPort", conf.get("hbase.zookeeper.property.clientPort"))
Pig.set("hbase.client.write.buffer","5242880")

######## GLOBAL ##########
# mapping between the loader and the export data types
data = {
        'ssm_controlled': 'ssm',
        'ssm_open': 'ssm',
        'clinical': 'donor',
        'clinicalsample' : 'donor',
        'cnsm' : 'cnsm',
        'jcn' : 'jcn',
        'meth_seq' : 'meth_seq',
        'meth_array' : 'meth_array',
        'mirna_seq' : 'mirna_seq',
        'stsm' : 'stsm',
        'pexp' : 'pexp',
        'exp_seq' : 'exp_seq',
        'exp_array' : 'exp_array',
        'sgv_controlled' : 'sgv'
       }

# mapping from short name to long name for each data type
longname = {
        'ssm_controlled' : 'simple_somatic_mutation.controlled',
        'ssm_open' : 'simple_somatic_mutation.open',
        'clinical' : 'clinical',
        'clinicalsample' : 'clinicalsample',
        'cnsm' : 'copy_number_somatic_mutation',
        'jcn' : 'splice_variant',
        'meth_seq' : 'meth_seq',
        'meth_array' : 'meth_array',
        'mirna_seq' : 'mirna_seq',
        'stsm' : 'structural_somatic_mutation',
        'pexp' : 'protein_expression',
        'exp_seq' : 'exp_seq',
        'exp_array' : 'exp_array',
        'sgv_controlled' : 'simple_germline_variation.controlled'
       }

######## Utility ##################
def touch(fname, times=None):
    fhandle = open(fname, 'a')
    try:
        os.utime(fname, times)
    finally:
        fhandle.close()