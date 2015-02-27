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
package org.icgc.dcc.etl.db.importer.project;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl.db.importer.util.Importers.getLocalMongoClientUri;

import java.util.Set;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.client.api.ICGCClientConfig;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.common.test.mongodb.EmbeddedMongo;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import com.mongodb.MongoClientURI;

@Slf4j
public class ProjectImporterTest {

  /**
   * Constants.
   */
  private static final String MONGO_DB_NAME = "project-importer-test";
  private static final String MONGO_RELEASE_PROJECT_DB_NAME = MONGO_DB_NAME + "."
      + ReleaseCollection.PROJECT_COLLECTION.getId();

  private static final ICGCClientConfig CONFIG = ICGCClientConfig.builder()
      .cgpServiceUrl("https://***REMOVED***/ud_oauth/1/search")
      .consumerKey("***REMOVED***")
      .consumerSecret("***REMOVED***")
      .accessToken("***REMOVED***")
      .accessSecret("***REMOVED***")
      .strictSSLCertificates(false)
      .requestLoggingEnabled(false)
      .build();

  @Rule
  public final static EmbeddedMongo embeddedMongo = new EmbeddedMongo();

  @Test
  public void testImport() {
    Set<String> includedProjects = ImmutableSet.<String> of("ALL-US");

    val importer =
        new ProjectImporter(CONFIG, getLocalMongoClientUri(embeddedMongo.getPort(),
            MONGO_DB_NAME));
    importer.execute();

    assertImport(includedProjects);
  }

  private static void assertImport(Set<String> includedProjects) {
    val projectsCollection =
        getProjectsCollection(getLocalMongoClientUri(embeddedMongo.getPort(), MONGO_RELEASE_PROJECT_DB_NAME));
    val projectsQuantity = projectsCollection.count();

    assertThat(projectsQuantity).isEqualTo(new Long(includedProjects.size()));
    log.info("Project: {}", projectsCollection.findOne().as(JsonNode.class));
  }

  private static MongoCollection getProjectsCollection(MongoClientURI mongoClientUri) {
    val jongo = getJongo(mongoClientUri);

    return jongo.getCollection(mongoClientUri.getCollection());
  }

  private static Jongo getJongo(MongoClientURI mongoClientUri) {
    val mongo = embeddedMongo.getMongo();
    val db = mongo.getDB(mongoClientUri.getDatabase());

    return new Jongo(db);
  }

}
