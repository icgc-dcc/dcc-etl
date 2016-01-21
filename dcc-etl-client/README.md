ICGC DCC - ETL Command Line Interface
===

Executes the ETL processing pipeline including the following:

*	[Load](../dcc-etl-loader/README.md)
*	[Indentify](https://github.com/icgc-dcc/dcc-id)
*	[Import](../dcc-etl-importer/README.md)
*	[Summarize](../dcc-etl-summarizer/README.md)
*	[Index](../dcc-etl-indexer/README.md)

Build
---

In the repository's root execute from the command line:

	mvn -am -pl dcc-etl-client package


Run
---

From the command line:

`java -jar target/dcc-etl-client-<version>.jar -c config.yaml -r <releaseName>`


Help
---

From the command line, type:

`java -jar target/dcc-etl-client-<version>.jar --help`


