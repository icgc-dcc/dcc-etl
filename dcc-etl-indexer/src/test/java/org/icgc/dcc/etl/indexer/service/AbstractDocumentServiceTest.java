/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.indexer.service;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;

import java.io.File;
import java.io.IOException;

import lombok.val;

import org.icgc.dcc.etl.indexer.core.Config;
import org.icgc.dcc.etl.indexer.core.DocumentService;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.icgc.dcc.etl.indexer.report.DocumentTypeReport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class AbstractDocumentServiceTest {

  /**
   * Test config.
   */
  static final String TEST_JOB_ID = "ICGC15-0-0";
  static final String TEST_RELEASE_NAME = "ICGC15";

  /**
   * Test input.
   */
  static final String TEST_FASTA_FILE = "/tmp/GRCh37.fasta";
  static final String TEST_MONGO_URI = "mongodb://localhost";
  static final String TEST_MONGO_DATABASE_NAME = "etl-test-release17-0-0";

  /**
   * Test output.
   */
  static final String TEST_ES_URI = "es://localhost:9300";
  static final String TEST_INDEX_NAME = "etl-indexer-test-release17-0-0";
  static final String TEST_OUTPUT_PATH = "target";

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  /**
   * Test file system.
   */
  File outputDir;

  /**
   * Test config.
   */
  Config config;

  /**
   * Class under test.
   */
  DocumentService service;

  @Before
  public void before() {
    this.outputDir = tmp.newFolder(TEST_OUTPUT_PATH);
    this.config = Config.builder()
        .releaseName(TEST_RELEASE_NAME)
        .mongoUri(getMongoUri(TEST_MONGO_URI, TEST_MONGO_DATABASE_NAME))
        .esUri(TEST_ES_URI)
        .fsUri("file:///")
        .indexName(TEST_INDEX_NAME)
        .fastaFile(new File(TEST_FASTA_FILE))
        .outputDir(outputDir.getAbsolutePath())
        .exportVCF(false)
        .build();
  }

  void test(DocumentType... types) throws IOException {
    service.writeDocuments(types);
    assertThat(outputDir.list()).isNotEmpty();
    report(types);
  }

  void report(DocumentType... types) {
    val report = new DocumentTypeReport(TEST_INDEX_NAME, newTransportClient(TEST_ES_URI));
    report.report(newArrayList(types));
  }

  static String getMongoUri(String mongoUri, String databaseName) {
    return String.format("%s/%s", mongoUri, databaseName);
  }

}
