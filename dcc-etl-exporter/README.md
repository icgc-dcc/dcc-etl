ICGC DCC - ETL Exporter
===

Exporter component for ETL pipeline which takes JSON files produced by the `dcc-etl-loader` and creates HFiles in HBase for consumption by the `dcc-downloader` component.

Build
---

From the command line:

	mvn package

This will create a tarball which contains the executables for running exporter.

System Requirements
---
- CDH 4.1.3 (Hadoop HDFS and MapReduce)
- HBase 0.93
- Pig 0.12.1 (Bundled)

Configurations
---

CDH Configurations
----
For performance reasons, HDFS should have dfs.client.read.shortcircuit enabled via Cloudera Manager and adding users (hbase and downloader) to the list of dfs.block.local-path-access.user

In order to run exporter under the user downloader, 

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

HBase should be configured with all regionservers to have 8GB of RAM.

Also, set the following parameters to the specified value:
	hbase.regionserver.handler.count: 50
	hbase.hregion.max.filesize: 1GB
	hbase.hregion.majorcompaction: 0

Pig Configurations
----
Since Pig included in CDH has jars that has a version of Guava that conflicts with the one that is used by the exporter, it is currently built by the exporter developer and uploaded to Artifactory. The exporter will bundled Pig from the artifactory into the distribution. 

Exporter Configurations
----
The core.py contains all the configuration parameters to execute the exporter. They override the default values in the scripts. For example root is the directory where exporter will use to export the static and dynamic downloads to (e.g. default to "/tmp/download"). 

There are some important configurations that are needed to modify when running it in a different cluster:
	hbase.zookeeper.quorum: the hostname which has zookeeper quorum installed
	loader: The Loader class that is used to read the input files from. (default to com.twitter.elephantbird.pig.load.LzoJsonLoader)

If for any reason, exporter is needed to run under a different user than downloader, you can specify that in the JOB_USER in the setenv.sh

Exporter Runtime Environment
--- 
Before to start exporter, please ensure the system has HBase, HDFS, MapReduce and Pig working. Please do the dryrun of using the following commands:

For HBase: 
	hbase shell

For HDFS:
	hdfs dfs -ls /

For Pig:
	${exporter-home-dir}/pig/bin/pig

