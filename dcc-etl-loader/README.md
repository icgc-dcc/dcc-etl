ICGC DCC - ETL Loader
===

DCC loader prepares submitted clinical data for further processing by executing following steps: 

- joins `primary`, `secondary` and `meta` feature types
- removes/obfuscates controlled data
- generates surrogate IDs to be used instead of the submitted IDs
- prepares artifacts to be used by [DCC Exporter](../dcc-etl-exporter/README.md)
- loads data into a DCC release MongoDB to be consumed by [DCC Summarizer](../dcc-etl-summarizer/README.md)


Build
---

In the repository's root execute from the command line:

	mvn -am -pl dcc-etl-loader package
