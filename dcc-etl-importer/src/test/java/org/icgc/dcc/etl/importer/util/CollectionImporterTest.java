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
package org.icgc.dcc.etl.importer.util;

import static com.google.common.collect.Iterables.size;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DONOR_COLLECTION;
import static org.icgc.dcc.etl.importer.util.Importers.getLocalMongoClientUri;

import java.io.File;

import lombok.SneakyThrows;

import org.icgc.dcc.common.test.mongodb.EmbeddedMongo;
import org.icgc.dcc.common.test.mongodb.MongoImporter;
import org.jongo.Jongo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.mongodb.DB;
import com.mongodb.MongoClientURI;

public class CollectionImporterTest {

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
    CollectionImporter importer =
        new CollectionImporter(getTargetDatabaseUri(), getTargetDatabaseUri(),
            Optional.<DocumentFilter> absent(), Optional.<DocumentFilter> absent());
    importer.import_(DONOR_COLLECTION.getId());

    assertThat(size(getDonors())).isEqualTo(0);
  }

  private Iterable<ObjectNode> getDonors() {
    return new Jongo(embeddedMongo.getMongo().getDB(getDatabaseName()))
        .getCollection(DONOR_COLLECTION.getId())
        .find()
        .as(ObjectNode.class);
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

  private MongoClientURI getTargetDatabaseUri() {
    return getLocalMongoClientUri(embeddedMongo.getPort(), getDatabaseName());
  }

  private String getDatabaseName() {
    return TEST_RELEASE_NAME;
  }

}
