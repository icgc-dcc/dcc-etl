ICGC DCC - ETL Exporter
===

Exporter component for ETL pipeline which takes JSON files produced by the `dcc-etl-loader` and creates HFiles in HBase for consumption by the `dcc-downloader` component.

Build
---

From the command line:

	mvn package

This will create a tarball which contains the executables for running exporter. Inside the tarball, the directory structure is as follows:

	bin/       #contains some helper scripts for executing exporter
	conf/      #contains logback and log4j configurations
	lib/       #contains pig runtime and exporter udfs
	logs/      #contains log files
	pig/       #contains the pig scripts for the exporter
	python/    #contains the python scripts for the exporter

`lib/dcc-etl-exporter.jar` is registered into the pig scripts during the runtime by the python scripts via parameter substitution under the variable name LIB. If by any reason you need to modify the name of the jar, you need to update it in the python scripts (exporter.py and bulkloader.py).

System Requirements
---
- CDH 4.1.3 (Hadoop HDFS and MapReduce)
- HBase 0.92.1
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
Since Pig included in CDH has jars that has a version of Guava that conflicts with the one that is used by the exporter, it is currently built by the exporter developer and uploaded to Artifactory. The exporter will bundled Pig from the artifactory into the distribution. The bundled pig can be found under lib/pig of the distribution. 

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

Run
---
To run exporter, use bin/export.sh. This is the script that exports both static and dynamic downloads. The syntax for running the script is as follows:

	bin/export.sh release_name loader_output_directory <data_types_to_export>

The release_name is the release name that is assigned by the run.
The loader_output_directory is the directory which contains the output from the loader (e.g. /icgc/overarch/ICGC18/0/3/loader)
The data_types_to_export is optional. The current supported data types are: ssm_open,ssm_controlled,sgv_controlled,pexp,mirna_seq,meth_seq,meth_array,jcn,exp_seq,exp_array,clinical,clinicalsample,cnsm and stsm. If it is not specified, it is currently assumed to be all data types.

The script will then execute static_export.sh and dynamic_export.sh in parallel and then each of the script will launch pig concurrently. For convenience, pig is ran via python/jython integration. All other java classes can be called via Pig UDF. 

Normally, the above is what is needed to run exporter but sometimes there is a need to export manually. For example, we want to export only ssm_open and ssm_controlled. Currently, loader is unable to perform that during runtime. 

	bin/export.sh release_name loader_output_directory ssm_open,ssm_controlled

It is also possible to run only dynamic export by:

	bin/dynamic_export.sh release_name loader_output_directory ssm_open,ssm_controlled

or just static export by:

	bin/static_export.sh release_name loader_output_directory ssm_open,ssm_controlled

Generally speaking, export.sh will run exporter.py via pig and then exporter.py will call static.pig and dynamic.pig. At the end of the dynamic export, HFiles generated by toHFile class are uploaded to the HBase using bulkloader.py which has two functions: load-balancing (by calling bucket.pig) and incremental upload to HBase (by calling org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles). 

There are other scripts that are only used for maintenance/operational purposes. backup.py is used to backup the static and dynamic export to a backup location (specified by root_backup in core.py).

Development
---
To add a new data type to be exported, modify the setenv.sh by adding the new data types to the datatypes variable:

	declare -a datatypes="ssm_open,ssm_controlled,sgv_controlled,pexp,mirna_seq,meth_seq,meth_array,jcn,exp_seq,exp_array,clinical,clinicalsample,cnsm,stsm,<new_data_type>"

Also make sure the associated pig scripts for the new data types are implemented and are check-in into the correct directory under pig/<new_data_type>. It is currently assumed that two pig scripts are provided for each new data types, static.pig (for static export) and dynamic.pig (for dynamic export) under pig/<new_data_type>.

