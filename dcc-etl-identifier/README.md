ICGC DCC - ETL Identifier
===

Dropwizard based web service that generates ICGC surrogate keys based on provided submission business keys. The results are stored in PostgreSQL for efficient lookup.

Setup
---

Install PostgreSQL 9.2.4:

[http://www.postgresql.org/download/](http://www.postgresql.org/download/)

Build
---

From the command line:

	mvn package

Run
---
The configurations for the dcc-etl-identifier are stored in the `conf/settings.yml`.
Make sure that the database.url in `settings.yml` is pointing to a valid database. 

To prepare the database, apply the `src/main/sql/schema.sql` into the database. Ensure that the `dcc` user has the right privileges to access the tables by executing the command in the file's comments. 

To start the service, execute `bin/identifier start`.
