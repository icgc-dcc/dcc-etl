/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.icgc.dcc.etl.loader;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.icgc.dcc.common.core.dcc.Component.LOADER;
import static org.icgc.dcc.common.core.dcc.DccResources.getCodeListsDccResource;
import static org.icgc.dcc.common.core.dcc.DccResources.getDictionaryDccResource;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.common.core.util.Optionals.ABSENT_STRING;
import static org.icgc.dcc.common.core.util.Strings2.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.Strings2.NOT_APPLICABLE;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.recursivelyDeleteDirectoryIfExists;
import static org.icgc.dcc.common.test.Tests.FS_DIR_NAME;
import static org.icgc.dcc.common.test.Tests.INPUT_DIR_NAME;
import static org.icgc.dcc.common.test.Tests.MAVEN_TEST_RESOURCES_DIR;
import static org.icgc.dcc.common.test.Tests.MONGO_DIR_NAME;
import static org.icgc.dcc.common.test.Tests.MONGO_PORT;
import static org.icgc.dcc.common.test.Tests.OUTPUT_DIR_NAME;
import static org.icgc.dcc.common.test.Tests.PROJECT1;
import static org.icgc.dcc.common.test.Tests.PROJECT2;
import static org.icgc.dcc.common.test.Tests.PROJECT3;
import static org.icgc.dcc.common.test.Tests.REFERENCE_DIR_NAME;
import static org.icgc.dcc.common.test.Tests.TEST_HOST;
import static org.icgc.dcc.common.test.Tests.getTestJobId;
import static org.icgc.dcc.common.test.Tests.getTestWorkingDir;
import static org.icgc.dcc.common.test.mongodb.JsonUtils.normalizeDumpFile;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Set;

import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.dcc.AppUtils;
import org.icgc.dcc.common.core.dcc.Component;
import org.icgc.dcc.common.core.util.Protocol;
import org.icgc.dcc.common.core.util.URIs;
import org.icgc.dcc.common.hadoop.fs.DccFileSystem2;
import org.icgc.dcc.common.hadoop.util.HadoopCompression;
import org.icgc.dcc.common.test.mongodb.JsonUtils;
import org.icgc.dcc.etl.loader.factory.LoaderServiceFactory;
import org.icgc.dcc.etl.loader.service.LoaderService;
import org.icgc.dcc.id.client.util.HashIdClient;

import com.google.common.base.Optional;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.wordnik.system.mongodb.SnapshotUtil;

/**
 * Helper class used by both the local and cluster loader integration tests.
 */
@Slf4j
public class LoaderIntegrationTestHelper {

  /**
   * Uses version specified in pom.
   */
  private static final URL DICTIONARY_URL = getDictionaryDccResource();
  private static final URL CODELISTS_URL = getCodeListsDccResource();
  private static final Component TESTED_COMPONENT = LOADER;
  private static final String TEST_JOB_ID = getTestJobId(TESTED_COMPONENT);
  private static final Set<String> PROJECT_KEYS = newLinkedHashSet(newArrayList(PROJECT1, PROJECT2, PROJECT3));

  /**
   * File system constants.
   */
  private static final String INPUT_DIR = PATH.join(MAVEN_TEST_RESOURCES_DIR, INPUT_DIR_NAME);
  private static final String REFERENCE_DIR = PATH.join(MAVEN_TEST_RESOURCES_DIR, REFERENCE_DIR_NAME);
  private static final String FS_REFERENCE_DIR = PATH.join(REFERENCE_DIR, FS_DIR_NAME);
  private static final String MONGO_REFERENCE_DIR = PATH.join(REFERENCE_DIR, MONGO_DIR_NAME);
  private static final String TEST_FS_DIR = getTestWorkingDir(LOADER);

  /**
   * Typically the submission system's FS.
   */
  public static final String TEST_FS_INPUT_DIR = PATH.join(TEST_FS_DIR, INPUT_DIR_NAME);

  /**
   * Typically the ETL output FS.
   */
  private static final String TEST_FS_OUTPUT_DIR = PATH.join(TEST_FS_DIR, OUTPUT_DIR_NAME);

  private static final String DEFAULT_INPUT_DIR = PATH.join(TEST_FS_INPUT_DIR, TEST_JOB_ID);
  private static final String OUTPUT_DIR = PATH.join(TEST_FS_OUTPUT_DIR, TEST_JOB_ID);
  private static final String MONGO_DUMPS_OUTPUT_DIR = PATH.join(TEST_FS_OUTPUT_DIR, MONGO_DIR_NAME);

  /**
   * Test configuration.
   */
  @Setter
  private String fsUrl;
  private String releaseMongoUri;
  private String identifierServiceUri;
  private String identifierClientClassName;
  private boolean filterAllControlled;
  private int maxConcurrentFlows;

  @Setter
  private Map<String, String> hadoop;
  private FileSystem fs;

  /**
   * Class under test.
   */
  private LoaderService service;

  /**
   * Perform a clean release with a single validated project
   */
  @SneakyThrows
  public void setUp() {
    configure();
    cleanStorage();
    importSubmissionFileSystemStructure();
  }

  /**
   * Execute the loader on an integration test data set and compares the results to verified set of output files.
   */
  public void testLoader(String description) {
    log.info("{}", repeat("-", 80));
    log.info("[{}] Integration testing loader...", description);
    log.info("{}", repeat("-", 80));
    service.loadRelease(
        TEST_JOB_ID,
        DICTIONARY_URL,
        CODELISTS_URL,
        PROJECT_KEYS,
        DEFAULT_INPUT_DIR,
        OUTPUT_DIR);

    exportReleaseDb();
    verifyReleaseDb();
    verifyExportedFiles();
  }

  @SneakyThrows
  private void configure() {
    // Update with additional runtime values
    releaseMongoUri = getMongoUri();
    identifierServiceUri = NOT_APPLICABLE;
    identifierClientClassName = HashIdClient.class.getName();
    filterAllControlled = false;
    maxConcurrentFlows = 10;
    val identifierAuthToken = EMPTY_STRING;

    // Create class under test; specify 'Lists.newArrayList("project2")' for instance to only process 'project2'
    service = LoaderServiceFactory.createService(
        fsUrl, HadoopCompression.NONE.name(),
        releaseMongoUri, hadoop,
        identifierClientClassName, identifierServiceUri, identifierAuthToken,
        filterAllControlled, maxConcurrentFlows);

    // Configure the filesystem
    fs = FileSystem.get(new URI(fsUrl), new Configuration());

    // So as to avoid loading libraries that may not be available
    AppUtils.setTestEnvironment();
  }

  /**
   * Cleans persistent storage.
   */
  @SneakyThrows
  private void cleanStorage() {
    // Remove the root file system
    recursivelyDeleteDirectoryIfExists(fs, new Path(TEST_FS_DIR));

    // Drop test databases
    MongoClientURI uri = new MongoClientURI(releaseMongoUri);
    MongoClient mongo = new MongoClient(uri);
    mongo.dropDatabase(getDatabaseName());
  }

  /**
   * Uploads a valid submission.
   * <p>
   * TODO: Make it use {@link DccFileSystem2}...
   * <p>
   * TODO: Merge with equivalent in EtlIntegrationTest
   */
  @SneakyThrows
  private void importSubmissionFileSystemStructure() {
    log.info("Copying '{}' to '{}'", INPUT_DIR, DEFAULT_INPUT_DIR);
    fs.copyFromLocalFile(false, true, new Path(INPUT_DIR), new Path(DEFAULT_INPUT_DIR));
  }

  /**
   * Exports the release database to the file system.
   */
  @SneakyThrows
  private void exportReleaseDb() {
    File loaderExportDir = new File(MONGO_DUMPS_OUTPUT_DIR);
    if (loaderExportDir.exists()) {
      deleteDirectory(loaderExportDir);
    }

    loaderExportDir.mkdirs();

    exportDb(getDatabaseName(), MONGO_DUMPS_OUTPUT_DIR);
  }

  /**
   * Verifies the release database against of manually validated reference files.
   */
  private void verifyReleaseDb() {
    for (File expectedFile : new File(MONGO_REFERENCE_DIR).listFiles()) {
      compareToReference(expectedFile, new File(MONGO_DUMPS_OUTPUT_DIR, expectedFile.getName()));
    }
  }

  /**
   * Verifies the exported files against of manually validated reference files.
   */
  private void verifyExportedFiles() {
    for (File typeDir : new File(FS_REFERENCE_DIR).listFiles()) {
      if (typeDir.isFile()) {
        continue;
      }
      for (File expectedFile : typeDir.listFiles()) {
        String actualTypeDir = PATH.join(OUTPUT_DIR, typeDir.getName());
        String fileName = expectedFile.getName();
        compareToReference(
            expectedFile,
            new File(actualTypeDir, fileName));
      }
    }
  }

  private void compareToReference(File expectedFile, File actualFile) {
    normalizeDumpFile(expectedFile);
    normalizeDumpFile(actualFile);

    log.info("Comparing: {} {}", expectedFile, actualFile);
    JsonUtils.assertJsonFileEquals(expectedFile, actualFile);
  }

  /**
   * Export all collections in {@code dbName} to {@code outputDir} as serialized sequence files of JSON objects.
   */
  private static void exportDb(String dbName, String outputDir) {
    log.info("Exporting DB '{}' to '{}'", dbName, outputDir);

    SnapshotUtil.main("-d", dbName, "-o", outputDir, "-J");
  }

  private String getDatabaseName() {
    return TEST_JOB_ID;
  }

  private static final String getMongoUri() {
    return getMongoUri(ABSENT_STRING);
  }

  private static final String getMongoUri(Optional<String> databaseName) {
    return URIs.getUriString(Protocol.MONGODB, TEST_HOST, MONGO_PORT, databaseName);
  }

}
