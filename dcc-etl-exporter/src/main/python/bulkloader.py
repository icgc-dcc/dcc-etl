#!/usr/bin/python
from org.apache.pig.scripting import *
from org.icgc.dcc.downloader.core import SchemaUtil
from subprocess import call
import os, os.path, sys
import getopt
sys.path.append(os.path.dirname(sys.argv[0]))
########CONTROL SETTINGS###########
from core import *

params = {
          'DEFAULT_PARALLEL': 60,
          'NUM_REGION' : 60
         }

########HADOOP JOB CONF SETTINGS###########
Pig.set("mapred.child.java.opts", "-Xms2G -Xmx8G")

####### DATA PROCESSING ###########
def bulkloadData(type):

  tmp_dynamic = tmp_dynamic_root + "/" + release + "/" + type
  params["KEYSPACE"] = tmp_dynamic + '/part-*'
  params["LIB"] = default_exporter_src + '/../lib/dcc-etl-exporter.jar'
  params["DATATYPE"] = type

  tmp_hfile = tmp_hfile_root + "/" + release + "/" + type 

  if upload is None :
    tablename = SchemaUtil.getDataTableName(type, release);
    SchemaUtil.createArchiveTable();
    SchemaUtil.createMetaTable(SchemaUtil.getMetaTableName(release)); 
  elif upload == '' :
    tablename = type
  else :
    tablename = SchemaUtil.getDataTableName(type, upload);

  print 'Bulkloading to table: ' + tablename
  params["TABLE_NAME"] = tablename

  if not(SchemaUtil.isTableExists(tablename)) :
    tmp_bucket = tmp_bucket_root + "/" + release + "/" + type
    Pig.fs("rm -r -skipTrash " + tmp_bucket)
    params["TMP_BUCKET_DIR"] = tmp_bucket
    # presplit regions evenly for new releases 
    print 'creating table with split: ' + tablename
    inittable = Pig.compileFromFile('load-balance-' + type , default_exporter_src + '/bucket.pig')
    bound  = inittable.bind(params)
    stats = bound.runSingle()

    if not stats.isSuccessful():
      print 'skip bulkloading data type: ' + type
      touch(logfile)
      for errMsg in stats.getAllErrorMessages():
              print errMsg
    else :
      # bulk load to tablename
      #try :
      os.system("HADOOP_USER_NAME=hdfs hdfs dfs -chown -R hbase " + tmp_hfile)
      os.system("HADOOP_USER_NAME=hbase hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles " + tmp_hfile + " " + tablename)
      os.system("HADOOP_USER_NAME=hdfs hdfs dfs -chown -R downloader " + tmp_hfile)

  #SchemaUtil.majorCompact(tablename)
  #except :
  #  print 'Pig job failed: bulk loading failed for data type: ' + type
  #  raise 'failed'

  #print 'Pig job succeeded'
  
if __name__ == "__main__":
  try:
    opts, args = getopt.getopt(sys.argv[1:],"hd:e:r:u:l:",["type=","exporter=","release=", "upload="])
  except getopt.GetoptError:
    print 'bulkloader.py -d <data type separated by comma> [-e <pig script directory>] [-r <release>] [-u <uploaded release>]'
    sys.exit(2)
    
  dataTypes = []
  logfile = "/tmp/exporter.ec"
  for opt, arg in opts:
    if opt == '-h':
      print 'bulkloader.py -d <data type separated by comma> [-e <pig script directory>] [-r <release>] [-u <uploaded release>]' 
    elif opt in ("-d", "--type"):
       dataTypes = arg.split(',')
    elif opt in ("-e", "--exporter"):
       default_exporter_src = arg
    elif opt in ("-r", "--release"):
       release = arg
    elif opt in ("-l", "--log"):
      logfile = arg
    elif opt in ("-u", "--upload"):
       if arg=="current":
         upload=''
       else:
         upload = arg

  for type in dataTypes:
    bulkloadData(type)
