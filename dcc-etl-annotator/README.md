ICGC DCC - ETL Annotator
===

This is the ETL annotation service. It is responsible for:

- Generating functional annotations based on ICGC SSM / SGV primary files
- Exporting the produced annotations to ICGC SSM / SGV secondary files

Build
---

In the repository's root execute from the command line:

	mvn -am -pl dcc-etl-annotator package


Run
---

```bash
java -jar dcc-etl-annotator-[version].jar
```
