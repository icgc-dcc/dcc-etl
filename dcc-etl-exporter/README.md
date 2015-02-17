ICGC DCC - ETL Exporter
===

Exporter component for ETL pipeline which takes JSON files produced by the `dcc-etl-loader` and creates HFiles in HBase for consumption by the `dcc-downloader` component.

Build
---

From the command line:

	mvn package

System Requirements
---
- CDH 4.1.3 (Hadoop HDFS and MapReduce)
- HBase 0.93
- Pig 0.12.1 (Bundled)

Configurations
---

CDH Configurations
----
- enable dfs.client.read.shortcircuit via Cloudera Manager 

Add the following users into dfs.block.local-path-access.user
hbase, downloader
enable dfs.client.read.shortcircuit

HBase Configurations
----

HBase client configuration should be deployed via Cloudera Manager to the node that is going to execute the exporter. 
From Cloudera Manager, add the following in the hbase-site.xml under the HBase Service Configuration Safety Valve:

<property>
	<name>dfs.client.read.shortcircuit</name>
	<value>true</value>
</property>

<property>
	<name>hbase.regionserver.checksum.verify</name>
	<value>true</value>
</property>
increase all regionservers to have 8GB of RAM

Add filter.jar to hbase path:
HBASE_CLASSPATH="/nfs/oozie/jlam/dcc-downloader/hbase-lib/filter.jar”

Pig Configurations
----
Since Pig included in CDH has jars that has a version of Guava that conflicts with the one used by the exporter, it is currently built by the exporter developer and uploaded to Artifactory. The exporter will bundled Pig in the distribution. 


Exporter Configurations
----
core.py
setenv.sh
hbase-site.xml
