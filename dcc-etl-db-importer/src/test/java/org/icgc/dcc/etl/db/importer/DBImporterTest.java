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
package org.icgc.dcc.etl.db.importer;

import static org.icgc.dcc.etl.core.config.Utils.createICGCConfig;
import static org.icgc.dcc.etl.db.importer.util.Importers.getLocalMongoClientUri;

import java.io.File;
import java.util.Arrays;

import lombok.SneakyThrows;
import lombok.val;

import org.icgc.dcc.common.test.mongodb.EmbeddedMongo;
import org.icgc.dcc.common.test.mongodb.MongoImporter;
import org.icgc.dcc.etl.core.config.EtlConfig;
import org.icgc.dcc.etl.core.config.EtlConfigFile;
import org.icgc.dcc.etl.db.importer.cli.CollectionName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.mongodb.DB;

public class DBImporterTest {

  /**
   * Test data.
   */
  private static final String TEST_RELEASE_NAME = "importer";
  private static final String TEST_DATA_DIR = "src/test/resources/fixtures";

  /**
   * Test environment.
   */
  @Rule
  public final EmbeddedMongo embeddedMongo = new EmbeddedMongo();

  @Before
  public void setUp() {
    importData();
  }

  @Test
  @SneakyThrows
  public void testExecute() {

    String configFilePath = "";
    EtlConfig config = EtlConfigFile.read(new File(configFilePath));

    val mongoUri = config.getGeneMongoUri();

    val dbImporter = new DBImporter(getLocalMongoClientUri(mongoUri),
        createICGCConfig(config));

    val collections = Arrays.asList(CollectionName.values());
    dbImporter.import_(collections);

  }

  private void importData() {
    MongoImporter importer = new MongoImporter(getSourceDirectory(), getTargetDatabase());
    importer.execute();
  }

  private File getSourceDirectory() {
    return new File(TEST_DATA_DIR);
  }

  private DB getTargetDatabase() {
    return embeddedMongo.getMongo().getDB(getDatabaseName());
  }

  private String getDatabaseName() {
    return TEST_RELEASE_NAME;
  }

}
