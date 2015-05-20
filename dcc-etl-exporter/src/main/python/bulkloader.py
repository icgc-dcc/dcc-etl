#!/usr/bin/python
from org.apache.pig.scripting import *
from org.icgc.dcc.downloader.core import SchemaUtil
from org.icgc.dcc.etl.exporter.core import LoadBalancer
from subprocess import call
import os, os.path, sys
import getopt
sys.path.append(os.path.dirname(sys.argv[0]))
########CONTROL SETTINGS###########
from core import *

########HADOOP JOB CONF SETTINGS###########
Pig.set("mapred.child.java.opts", "-Xms2G -Xmx8G")

####### DATA PROCESSING ###########
def bulkloadData(type):
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

  if not(SchemaUtil.isTableExists(tablename)) :
    LoadBalancer.run(tablename, tmp_hfile)

  # bulk load to tablename
  os.system("HADOOP_USER_NAME=hdfs hdfs dfs -chown -R hbase " + tmp_hfile)
  os.system("HADOOP_USER_NAME=hbase java -cp `hbase classpath` -Xmx8G org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=9999 " + tmp_hfile + " " + tablename)
  os.system("HADOOP_USER_NAME=hdfs hdfs dfs -chown -R downloader " + tmp_hfile)
  print 'Pig job succeeded'
  
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
