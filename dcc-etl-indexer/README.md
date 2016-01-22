ICGC DCC - ETL Indexer
===

DCC indexer reads a DCC release MongoDB database, transforms the normalized documents into ElasticSearch documents for indexing and indexes them into an ElasticSearch cluster.

Build
---

In the repository's root execute from the command line:

	mvn -am -pl dcc-etl-indexer package
