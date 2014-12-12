ICGC DCC - ETL Exporter
===

Exporter component for ETL pipeline which takes JSON files produced by the `dcc-etl-loader` and creates HFiles in HBase for consumption by the `dcc-downloader` component.

Build
---

From the command line:

	mvn package
