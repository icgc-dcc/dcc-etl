#!/usr/bin/python
from org.apache.pig.scripting import *
import os.path, sys
import getopt
import time
sys.path.append(os.path.dirname(sys.argv[0]))
########CONTROL SETTINGS###########
from core import *

####### DATA PROCESSING ###########
def backupData():
  #create backup directory locally
  Pig.fs("mkdir " + "file:///" + root_backup)
  final_out_static = root_out_static + "/" + release
  final_out_dynamic = out_dynamic + "/" + release

  out_backup_static = root_backup + "/static/"
  out_backup_dynamic = root_backup + "/dynamic/"

  Pig.fs("mkdir " + "file:///" + out_backup_static)
  Pig.fs("mkdir " + "file:///" + out_backup_dynamic)

  Pig.fs("get " + final_out_static + " " + out_backup_static);
  Pig.fs("get " + final_out_dynamic + " " + out_backup_dynamic);
  
if __name__ == "__main__":
  try:
    opts, args = getopt.getopt(sys.argv[1:],"h:r:",["release="])
  except getopt.GetoptError:
    print 'exporter.py [-r <release>]'
    sys.exit(2)
    
  dataTypes = []
  for opt, arg in opts:
    if opt == '-h':
      print 'exporter.py [-r <release>]' 
    elif opt in ("-r", "--release"):
       release = arg

  backupData()
