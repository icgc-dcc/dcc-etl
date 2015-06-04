ICGC DCC - Heliotrope Resource Bundle
===

This bundle contains several static resources used by ETL pipeline.

### Last Updated:
- `cancer_gene_census.tsv`: *May 28th, 2015*.
- `pathway_hier.txt`: *November 28th, 2014, release 52*.
- `uniprot_2_reactome.txt`: *May 28th, 2015*.
- `pathway_2_summation.txt`: *May 28th, 2015*.
- `genes.bson`: *February 5, 2015*, removed genes with bad chromosomes.
- `gene2xml`: *February 5, 2015*.


### 1. Updating Resources:

#### 1.1. Prepare.
Download and extract the latest heliotrope jar from [Artifactory.](https://seqwaremaven.oicr.on.ca/artifactory/simple/dcc-dependencies/org/icgc/dcc/dcc-heliotrope) Make a copy of the folder and update the version.

```bash
dcc_heliotrope_current_version='12'
dcc_heliotrope_next_version='13'

artifactory_dependency_url='https://seqwaremaven.oicr.on.ca/artifactory/dcc-dependencies/org/icgc/dcc/'
dcc_heliotrope_name='dcc-heliotrope'

dcc_heliotrope_current_dist_folder_name=${dcc_heliotrope_name}-${dcc_heliotrope_current_version}
dcc_heliotrope_next_dist_folder_name=${dcc_heliotrope_name}-${dcc_heliotrope_next_version}

dcc_heliotrope_current_dist_file_name=${dcc_heliotrope_current_dist_folder_name}.jar
dcc_heliotrope_next_dist_file_name=${dcc_heliotrope_next_dist_folder_name}.jar

mkdir /tmp/${dcc_heliotrope_current_dist_folder_name}
cd /tmp/${dcc_heliotrope_current_dist_folder_name}
wget ${artifactory_dependency_url}${dcc_heliotrope_name}/${dcc_heliotrope_current_version}/${dcc_heliotrope_current_dist_file_name}
jar -xf ${dcc_heliotrope_current_dist_file_name}
rm -f ${dcc_heliotrope_current_dist_file_name}

cd ..
mkdir /tmp/${dcc_heliotrope_next_dist_folder_name}
cp -R ${dcc_heliotrope_current_dist_folder_name}/* ${dcc_heliotrope_next_dist_folder_name}
cd /tmp/${dcc_heliotrope_next_dist_folder_name}
```

#### 2. Update the files.

##### 2.1. Reactome Pathway Resources

Refer to this [wiki page](https://wiki.oicr.on.ca/display/DCCSOFT/Reactome+Pathway+Update+-+Nov+2014) for complete information.

```bash
curl http://www.reactome.org/ReactomeRESTfulAPI/RESTfulWS/pathwayHierarchy/homo+sapiens > pathway_hierarchy.txt
curl http://www.reactome.org/download/current/UniProt2Reactome.txt > uniprot_2_reactome.txt
curl http://www.reactome.org/download/current/pathway2summation.txt > pathway_2_summation.txt
```

##### 2.2. Cancer Gene Census

Download Cancer Gene Census file from [COSMIC](https://cancer.sanger.ac.uk/census). Save it as cancer_gene_census.tsv. You need to register and login to download the file.

##### 2.3. gene2xml and gene.bson
These 2 binary files rarely (if ever) get updated. Cosult others to see if any update is required and hope that the answer is no. 


### 2. Verify Resources.

Unfortunately, the updated files are usually not in the right format or consistency. So some manual work is needed to make them compatible with ETL component. Based on previous experiences, these are some items to look out for:

- `cancer_gene_census.tsv` file might have csv header. Just replace the commas with tab character in a text editor.

- Reactome names are present in `pathway_hierarchy.txt` but missing from `pathway_2_summation.txt`. You'd need to resolve them using `uniprot_2_reactome.txt`. Start by copying the lines with '???' from the end of previous `pathway_2_summation.txt` to the new verison. For each one of those, search for the REACT_[id] in the file to see if the data is provided in the current version. If so, delete the lines.

Currently, the following reactoem names are inconsistent between the reactome data files and have been resolved with other methods:

- The following reactome names are present in `pathway_hierarchy.txt` but missing from `pathway_2_summation.txt` and have been resolved using `uniprot_2_reactome.txt`:
  - PI3K Cascade
  - RNA Polymerase II Transcription
  - S6K1-mediated signalling
  - Switching of origins to a post-replicative state
  - mTOR signalling

- The following reactome names are present in `pathway_hierarchy.txt` but missing from `pathway_2_summation.txt` and `uniprot_2_reactome.txt` have been resolved using reactome.org website:
  - Acetylcholine Binding And Downstream Events
  - Cell Cycle
  - Cell junction organization
  - Mitotic G1-G1/S phases
  - Mitotic G2-G2/M phases
  - RNA Polymerase II Transcription
  - Regulation of mitotic cell cycle
  - Transmembrane transport of small molecules
  - mTORC1-mediated signalling

- The following reactome ids are present in `uniprot_2_reactome.txt` but missing from the other 2 files.
  - REACT_790
  - REACT_1451
  - REACT_330
  - REACT_2204
  - REACT_1156
  - REACT_329
  - REACT_22107
  - REACT_22201
  - REACT_1178
  - REACT_63
  - REACT_6772
  - REACT_1993
  - REACT_1156

### 3. Create and publish the new bundle.

You need to create the new bundle using new data and test that ETL is functioning properly with the new bundle.

#### 3.1. Get and update the pom file
```bash
cd /tmp
dcc_heliotrope_current_pom_file_name=${dcc_heliotrope_current_dist_folder_name}.pom
dcc_heliotrope_next_pom_file_name=${dcc_heliotrope_next_dist_folder_name}.pom
curl ${artifactory_dependency_url}${dcc_heliotrope_name}/${dcc_heliotrope_current_version}/${dcc_heliotrope_current_pom_file_name} > ${dcc_heliotrope_next_pom_file_name}
sed -i "s/<version>${dcc_heliotrope_current_version}<\/version>/<version>${dcc_heliotrope_next_version}<\/version>/" ${dcc_heliotrope_next_pom_file_name}
```

#### 3.2. Create the new jar
```bash
cd /tmp/${dcc_heliotrope_next_dist_folder_name}
jar cf ${dcc_heliotrope_next_dist_file_name} *
mv ${dcc_heliotrope_next_dist_file_name} ..
```

#### 3.3. Publish the artifact to your local maven repository to test.
```bash
cd /tmp/
mvn install:install-file -Dfile=${dcc_heliotrope_next_dist_file_name} -DpomFile=${dcc_heliotrope_next_pom_file_name}
```
### 4. Update and test ETL.

#### 4.1. Update version references in source code
Create a new feature branch and update the version in db-importer pom file in ```dcc-etl-db-importer/pom.xml``` and ```dcc-etl-db-importer/src/main/java/org/icgc/dcc/etl/db/importer/util/Importers.java```.

#### 4.2. Run db-importer tests
db-importer modules heavily depends on the heliotrope resource, so running the unit tests is the first step to catch issues with updates bundle. Run the tests and try to resolve the issues. After modifying the file, repeat steps 3.2 and 3.3 to recreate and republish the jar file.

```bash
cd dcc-etl-db-importer
mvn clean package
```

#### 4.3. Publish to Artifactory.
Now that the artifact passes local testing it needs to be published to central artifactory. This is also needed because ETL integration test expects the resource in artifactory. Use the [deploy page](http://seqwaremaven.oicr.on.ca/artifactory/webapp/deployartifact.html) and upload the final jar file.


#### 4.4. Run all ETL tests
```bash
cd dcc-etl
mvn clean package
```

### 6. Publish new ETL release.
Push the changes to repository and wait for Continuous Integration to verify the build. Optionally, publish the complete ETL release to artifactory.

### 7. Update this document.
Reflect the changes and their date.
