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

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_SET_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.etl.core.config.Utils.createICGCConfig;
import static org.icgc.dcc.etl.db.importer.util.Jongos.createJongo;

import java.io.File;
import java.util.Arrays;

import lombok.val;

import org.icgc.dcc.etl.core.config.EtlConfig;
import org.icgc.dcc.etl.core.config.EtlConfigFile;
import org.icgc.dcc.etl.db.importer.cli.CollectionName;
import org.jongo.Jongo;
import org.junit.Test;

import com.mongodb.MongoClientURI;

public class DBImporterTest {

  /**
   * Test settings.
   */
  private static final String TEST_CONFIG_FILE = "src/test/conf/config.yaml";
  private static final EtlConfig CONFIG = EtlConfigFile.read(new File(TEST_CONFIG_FILE));

  /**
   * Test environment.
   */
  private final Jongo jongo = createJongo(new MongoClientURI(CONFIG.getGeneMongoUri()));

  @Test
  public void testExecute() {
    val dbImporter = new DBImporter(CONFIG.getGeneMongoUri(), createICGCConfig(CONFIG));
    val collections = Arrays.asList(CollectionName.values());
    dbImporter.import_(collections);

    assertThat(getCollectionSize(PROJECT_COLLECTION.getId())).isGreaterThan(0);
    assertThat(getCollectionSize(GENE_COLLECTION.getId())).isGreaterThan(0);
    assertThat(getCollectionSize(GENE_SET_COLLECTION.getId())).isGreaterThan(0);
  }

  private long getCollectionSize(String collectionName) {
    return jongo.getCollection(collectionName).count();
  }

}
