/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.etl;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.Component.ETL;
import static org.icgc.dcc.common.core.DccResources.CODELISTS_JSON_FILE_NAME;
import static org.icgc.dcc.common.core.DccResources.DICTIONARY_JSON_FILE_NAME;
import static org.icgc.dcc.common.core.DccResources.getCodeListsDccResource;
import static org.icgc.dcc.common.core.DccResources.getDictionaryDccResource;
import static org.icgc.dcc.common.core.model.IndexType.DONOR_CENTRIC_TYPE;
import static org.icgc.dcc.common.core.model.IndexType.DONOR_TYPE;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseDatabase.GENOME;
import static org.icgc.dcc.common.core.model.ReleaseDatabase.IDENTIFICATION;
import static org.icgc.dcc.common.core.util.Joiners.COMMA;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.common.core.util.Optionals.ABSENT_STRING;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.recursivelyDeleteDirectoryIfExists;
import static org.icgc.dcc.common.test.Tests.ELASTIC_SEARCH_PORT;
import static org.icgc.dcc.common.test.Tests.FS_DIR_NAME;
import static org.icgc.dcc.common.test.Tests.INPUT_DIR_NAME;
import static org.icgc.dcc.common.test.Tests.MONGO_PORT;
import static org.icgc.dcc.common.test.Tests.TARGET_DIR_NAME;
import static org.icgc.dcc.common.test.Tests.TEST_FIXTURES_DIR;
import static org.icgc.dcc.common.test.Tests.TEST_HOST;
import static org.icgc.dcc.common.test.Tests.TEST_PATCH_NUMBER;
import static org.icgc.dcc.common.test.Tests.TEST_RELEASE_NUMBER;
import static org.icgc.dcc.common.test.Tests.TEST_RUN_NUMBER;
import static org.icgc.dcc.common.test.Tests.getTestJobId;
import static org.icgc.dcc.common.test.Tests.getTestReleasePrefix;
import static org.icgc.dcc.common.test.Tests.getTestWorkingDir;
import static org.icgc.dcc.etl.core.config.ICGCClientConfigs.createICGCConfig;
import static org.icgc.dcc.etl.loader.factory.LoaderServiceFactory.IDENTIFIER_CLIENT_CLASSNAME_PROPERTY;
import static org.icgc.dcc.etl.service.EtlService.EtlAction.IMPORT;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.icgc.dcc.common.client.api.ICGCClient;
import org.icgc.dcc.common.core.Component;
import org.icgc.dcc.common.core.mail.Mailer;
import org.icgc.dcc.common.core.model.IndexType;
import org.icgc.dcc.common.core.util.Protocol;
import org.icgc.dcc.common.core.util.URIs;
import org.icgc.dcc.common.core.util.URLs;
import org.icgc.dcc.common.hadoop.fs.FileSystems;
import org.icgc.dcc.common.test.es.ElasticSearchExporter;
import org.icgc.dcc.etl.client.ClientMain;
import org.icgc.dcc.etl.core.config.EtlConfig;
import org.icgc.dcc.etl.core.config.EtlConfigFile;
import org.icgc.dcc.etl.importer.Importer;
import org.icgc.dcc.id.client.util.HashIdClient;
import org.icgc.dcc.imports.core.model.ImportSource;
import org.icgc.dcc.imports.diagram.DiagramImporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EtlIntegrationTest {

  /**
   * Test configuration file.
   */
  public static final String TEST_CONFIG_FILE = "src/test/conf/config.yaml";
  private static final String TEST_GENES_IDS = "ENSG00000176105,ENSG00000156976,ENSG00000215529,ENSG00000204099";
  private static final String TEST_DIAGRAMS = "453279,1187000";

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  /**
   * s Sets key system properties before test initialization.
   */
  static {
    // See HADOOP-7489
    System.setProperty("java.security.krb5.realm", "OX.AC.UK");
    System.setProperty("java.security.krb5.kdc", "kdc0.ox.ac.uk:kdc1.ox.ac.uk");
  }

  /**
   * Test data.
   */
  private static final Component TESTED_COMPONENT = ETL;
  private static final String TEST_JOB_ID = getTestJobId(TESTED_COMPONENT);
  private static final String TEST_RELEASE_PREFIX = getTestReleasePrefix(TESTED_COMPONENT);
  private static final Set<String> PROJECT_KEYS = new ImmutableSet.Builder<String>()
      .add("OV-AU")
      .add("PACA-AU")
      .add("PEME-CA")
      .add("MALY-DE")
      .add("LAML-US")
      .build();

  /**
   * File system constants.
   */
  private static final String TEST_WORKING_DIR = getTestWorkingDir(TESTED_COMPONENT);
  private static final String INPUT_DIR = PATH.join(TEST_FIXTURES_DIR, INPUT_DIR_NAME);
  private static final String FS_INPUT_DIR = PATH.join(INPUT_DIR, FS_DIR_NAME);

  /**
   * Uses version specified in pom.
   */
  private static final String DICTIONARY_FILE_PATH = PATH.join(TEST_WORKING_DIR, DICTIONARY_JSON_FILE_NAME);
  private static final String CODELISTS_FILE_PATH = PATH.join(TEST_WORKING_DIR, CODELISTS_JSON_FILE_NAME);

  @Before
  public void setup() {
    // Only import some test gene and diagram ids to significantly speedup processing during testing
    System.setProperty(Importer.INCLUDED_GENE_IDS_SYSTEM_PROPERTY_NAME, TEST_GENES_IDS);
    System.setProperty(DiagramImporter.INCLUDED_REACTOME_DIAGRAMS, TEST_DIAGRAMS);
    System.setProperty(IDENTIFIER_CLIENT_CLASSNAME_PROPERTY, HashIdClient.class.getName());

    seedFathmmDB();
    seedGenomeDB();

    importSubmissionFileSystemStructure();

    cleanRelease();
    cleanIds();
    cleanIndex();
  }

  @Test
  public void testEtl() {
    ClientMain.main(
        "--config", TEST_CONFIG_FILE,
        "--job-id", TEST_JOB_ID,
        "--release-prefix", TEST_RELEASE_PREFIX,
        "--release-number", String.valueOf(TEST_RELEASE_NUMBER),
        "--patch-number", String.valueOf(TEST_PATCH_NUMBER),
        "--run-number", String.valueOf(TEST_RUN_NUMBER),
        "--dictionary", URLs.toFile(getDictionaryDccResource(), DICTIONARY_FILE_PATH),
        "--codelists", URLs.toFile(getCodeListsDccResource(), CODELISTS_FILE_PATH),
        "--projects", COMMA.join(PROJECT_KEYS),
        "--working-dir", TEST_WORKING_DIR);

    val indexName = getIndexName();
    val client = getIndexClient();

    // Ensure the index exists
    val exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
    assertThat(exists).isTrue();

    // Ensure that each index type has at least 1 document
    for (val indexType : IndexType.values()) {
      val typeName = indexType.getName();
      val count = client.prepareCount(indexName).setTypes(typeName).execute().actionGet().getCount();
      assertThat(count).isGreaterThan(0L);
    }
  }

  @Test
  @Ignore
  public void testEtlSpecific() {
    ClientMain.main(
        "-c", TEST_CONFIG_FILE,
        "-r", TEST_RELEASE_PREFIX,
        "-i", DONOR_TYPE.getName() + "," + DONOR_CENTRIC_TYPE.getName(),

        IMPORT.getId());
  }

  @After
  public void tearDown() {
    exportIndex();
  }

  private void seedGenomeDB() {

    EtlConfig config = EtlConfigFile.read(new File(TEST_CONFIG_FILE));
    val cgpClient = ICGCClient.create(createICGCConfig(config)).cgp();

    Mailer mailer = Mailer.builder().enabled(false).build();

    val dbImporter = new org.icgc.dcc.imports.client.core.Importer(
        getGenomeUri(),
        mailer,
        cgpClient);

    val collections = Arrays.asList(ImportSource.values());
    dbImporter.execute(collections);

  }

  @SneakyThrows
  private void seedFathmmDB() {
    // This is just to pass integration test, the real fathmm dataset
    // is large (20+ GB) and expensive to do a deploy-teardown cycle
    String jdbcURI =
        format("jdbc:h2:target/fathmm;MODE=PostgreSQL;INIT=runscript from '%s'",
            "../dcc-etl-importer/src/test/resources/sql/fathmm.sql");
    DBI dbi = new DBI(jdbcURI);
    Handle handle = dbi.open();
    handle.close();
  }

  /**
   * TODO: Merge with equivalent in LoaderIntegrationTest
   */
  @SneakyThrows
  private void importSubmissionFileSystemStructure() {
    val fs = FileSystems.getDefaultLocalFileSystem();
    log.info("Copying '{}' to '{}'", FS_INPUT_DIR, TEST_WORKING_DIR);
    recursivelyDeleteDirectoryIfExists(fs, new Path(TEST_WORKING_DIR));
    fs.copyFromLocalFile(false, true, new Path(FS_INPUT_DIR), new Path(TEST_WORKING_DIR));
  }

  private void exportIndex() {
    ElasticSearchExporter exporter = new ElasticSearchExporter(getIndexDirectory(), getIndexName(), getIndexClient());
    exporter.execute();
  }

  @SneakyThrows
  private void cleanIds() {
    log.info("Dropping {}...", getIdentificationUri());
    MongoClient releaseMongo = new MongoClient(getIdentificationUri());
    releaseMongo.dropDatabase(getIdentificationDbName());
  }

  @SneakyThrows
  private void cleanRelease() {
    log.info("Dropping {}...", getReleaseUri());
    MongoClient releaseMongo = new MongoClient(getReleaseUri());
    releaseMongo.dropDatabase(getDatabaseName());
  }

  private void cleanIndex() {
    IndicesAdminClient indices = getIndexClient().admin().indices();
    if (indices.prepareExists(getIndexName()).execute().actionGet().isExists()) {
      log.info("Dropping index: {}", getIndexName());
      indices.prepareDelete(getIndexName()).execute().actionGet();
    }
  }

  private MongoClientURI getReleaseUri() {
    return getMongoUri(getDatabaseName());
  }

  private String getProjectsDbName() {
    return getProjectsUri().getDatabase();
  }

  @SneakyThrows
  private DB getProjectsDb() {
    return new MongoClient(getProjectsUri()).getDB(getProjectsDbName());
  }

  private MongoClientURI getProjectsUri() {
    return getMongoUri(PROJECT_COLLECTION.getFullyQualifiedCollectionName());
  }

  private String getIdentificationDbName() {
    return getIdentificationUri().getDatabase();
  }

  private MongoClientURI getIdentificationUri() {
    return getMongoUri(IDENTIFICATION.getId());
  }

  private MongoClientURI getGenomeUri() {
    return getMongoUri(GENOME.getId());
  }

  private static String getIndexName() {
    return getDatabaseName().toLowerCase();
  }

  private File getIndexDirectory() {
    return new File(TARGET_DIR_NAME, getDatabaseName());
  }

  private static String getDatabaseName() {
    return TEST_JOB_ID;
  }

  @SneakyThrows
  private TransportClient getIndexClient() {
    URI esUri = getEsUri();
    return new TransportClient().addTransportAddress(
        new InetSocketTransportAddress(
            esUri.getHost(),
            esUri.getPort()));
  }

  private MongoClientURI getMongoUri(@NonNull final String path) {
    return new MongoClientURI(URIs.getUriString(Protocol.MONGODB, TEST_HOST, MONGO_PORT, Optional.of(path)));
  }

  private URI getEsUri() {
    return URIs.getUri(Protocol.ES, TEST_HOST, ELASTIC_SEARCH_PORT, ABSENT_STRING);
  }

}
