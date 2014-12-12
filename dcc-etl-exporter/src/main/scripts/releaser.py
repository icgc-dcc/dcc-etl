from org.icgc.dcc.downloader.core import RenameTable
from org.icgc.dcc.downloader.core import SchemaUtil
from org.icgc.dcc.downloader.core import ArchiverConstant
import os.path, sys
import getopt

backup_suffix = '.bk'

types = [ 'ssm_control', 
 	        'ssm_open',
          'clinical',
          'clinicalsample', 
          'cnsm',
          'jcn', 
          'meth',
          'mirna',
          'stsm',
          'pexp',
          'exp',
          'sgv'
        ]

def releaseMeta(releaseName):
  metaTableName = ArchiverConstant.META_TABLE_NAME
  metaReleaseName = SchemaUtil.getMetaTableName(releaseName)

  try:
    SchemaUtil.checkTableIntegrity(metaTableName)
  except:
    print "meta table does not exist..."

  try:
    SchemaUtil.checkTableIntegrity(metaReleaseName)
  except:
    print "Error"
    raise

  SchemaUtil.deleteTables(metaTableName + backup_suffix);
  
  RenameTable.exec(metaTableName, metaTableName + backup_suffix)
  RenameTable.exec(metaReleaseName, metaTableName)

  SchemaUtil.checkTableIntegrity(metaTableName)

def releaseData(type, releaseName):
  try:
    SchemaUtil.checkTableIntegrity(type);
  except:
    print type + " data table does not exist..."

  try:
    dataReleaseName = SchemaUtil.getDataTableName(type, release);
    SchemaUtil.checkTableIntegrity(dataReleaseName);

    SchemaUtil.deleteTables(type + backup_suffix)
    RenameTable.exec(type, type + backup_suffix)
    RenameTable.exec(dataReleaseName, type)
  except:
    print "Release: " + release + " has no data for data type: " + type


if __name__ == "__main__":
  try:
    opts, args = getopt.getopt(sys.argv[1:],"hr:d",["release=","type="])
  except getopt.GetoptError:
    print 'releaser.py -r <release name> [-d <data type>]'
    sys.exit(2)
    
  type = '';
  release = '';
  for opt, arg in opts:
    if opt == '-h':
      print 'releaser.py -r <release name> [-d <data type>]'
    elif opt in ("-d", "--type"):
       type = arg
    elif opt in ("-r", "--release"):
       release = arg
  try:
    if type == '':
      # release all data types
      releaseMeta(release)
      for type in types:
        releaseData(type, release)
    else :
      releaseData(type, release)
  except :
    sys.exit(2)

