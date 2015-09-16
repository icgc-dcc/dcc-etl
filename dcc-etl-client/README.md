ICGC DCC - ETL Command Line Interface
===

Executes the ETL processing pipeline including the following:

*	Load
*	Indentify
*	Import
*	Summarize
*	Index

Build
---

From the command line:

`mvn package`

Run
---

From the command line:

`java -jar target/dcc-etl-client-<version>.jar -c config.yaml -r <releaseName>`


Help
---

From the command line, type:

`java -jar target/dcc-etl-client-<version>.jar --help`


