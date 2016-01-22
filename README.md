ICGC DCC - ETL
===

Parent project of the ETL pipeline modules. 

*Note:* This project is soon to be replaced with [`dcc-release`](https://github.com/icgc-dcc/dcc-release) which greatly simplifies the required infrastructure and implementation.

The ETL project consumes clinical data submited by the users and prepares it to be consumed by the [DCC Portal application.](https://github.com/icgc-dcc/dcc-portal)

Build
---

From the command line:

	mvn package

Modules
---

Sub-system modules:

- [Client](dcc-etl-client/README.md)
- [Core](dcc-etl-core/README.md)
- [Annotator](dcc-etl-annotator/README.md)
- [Loader](dcc-etl-loader/README.md)
- [Exporter](dcc-etl-exporter/README.md)
- [Importer](dcc-etl-importer/README.md)
- [Summarizer](dcc-etl-summarizer/README.md)
- [Indexer](dcc-etl-indexer/README.md)

Additional Info
---
- [Running ETL](ETL.md)
- [Fathmm](Fathmm.md)

Copyright and license
---

 - [License](LICENSE.md)
 