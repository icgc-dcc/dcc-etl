ICGC DCC - ETL Annotator
===

This is the ETL annotation service. It is responsible for:

- Generating functional annotations based on ICGC SSM / SGV primary files
- Exporting the produced annotations to ICGC SSM / SGV secondary files

Build
---

From the command line:

```bash
mvn package
```

Run
---

```bash
java -jar dcc-etl-annotator-[version].jar
```
