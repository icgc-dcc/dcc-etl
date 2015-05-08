/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.db.importer.repo;

import static com.google.common.base.Stopwatch.createStarted;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.icgc.dcc.common.core.model.ReleaseCollection.FILE_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.etl.db.importer.util.Jongos.createJongo;

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.tcga.TCGAClient;
import org.icgc.dcc.etl.core.id.HashIdentifierClient;
import org.icgc.dcc.etl.core.id.IdentifierClient;
import org.icgc.dcc.etl.db.importer.cli.CollectionName;
import org.icgc.dcc.etl.db.importer.core.Importer;
import org.icgc.dcc.etl.db.importer.repo.cghub.CGHubImporter;
import org.icgc.dcc.etl.db.importer.repo.core.RepositoryContext;
import org.icgc.dcc.etl.db.importer.repo.pcawg.PCAWGImporter;
import org.icgc.dcc.etl.db.importer.repo.tcga.TCGAImporter;
import org.icgc.dcc.etl.db.importer.util.TransportClientFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClientURI;

/**
 * Importer for the ICGC "Data Repository" feature which imports file metadata from various external data sources.
 * 
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/JSON+structure+for+ICGC+data+repository
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/UI+-+The+new+ICGC+DCC+data+repository+-+Simplified+version+Phase+1
 */
@Slf4j
@RequiredArgsConstructor
public class RepositoryImporter implements Importer {

  /**
   * Configuration
   */
  @NonNull
  private final MongoClientURI mongoUri;
  private final String esUri = "es://localhost:9300";

  @Override
  public CollectionName getCollectionName() {
    return CollectionName.FILES;
  }

  @Override
  public void execute() {
    val watch = createStarted();

    // The business
    write();
    index();

    log.info("Finished importing repository in {}", watch);
  }

  private void write() {
    val context = createRepositoryContext();

    logBanner(" Importing PCAWG");
    val projectImporter = new PCAWGImporter(context);
    projectImporter.execute();

    logBanner(" Importing TCGA");
    val tcgaImporter = new TCGAImporter(context);
    tcgaImporter.execute();

    logBanner(" Importing CGHub");
    val cghubImporter = new CGHubImporter(context);
    cghubImporter.execute();
  }

  private void index() {
    // TODO: Externalize
    val indexName = "icgc-repository";
    val typeName = "file";
    val client = TransportClientFactory.newTransportClient(esUri);
    val jongo = createJongo(mongoUri);
    client.prepareDelete().setIndex(indexName).execute();

    val files = jongo.getCollection(FILE_COLLECTION.getId());
    for (val file : files.find().as(ObjectNode.class)) {
      file.remove("_id");
      client.prepareIndex(indexName, typeName).setSource(file.toString()).execute();
    }
  }

  private Map<String, String> getProjectPrimarySites() {
    // TODO: Use ICGC API?
    val jongo = createJongo(mongoUri);

    val map = ImmutableMap.<String, String> builder();
    val projects = jongo.getCollection(PROJECT_COLLECTION.getId());
    for (val project : projects.find().as(ObjectNode.class)) {
      val projectName = project.get("_project_id").textValue();
      val primarySite = project.get("primary_site").textValue();

      map.put(projectName, primarySite);
    }

    return map.build();
  }

  private RepositoryContext createRepositoryContext() {
    // TODO: Externalize
    val primarySites = getProjectPrimarySites();
    val identifierClient = createIdentifierClient();
    val tcgaClient = createTCGAClient();

    return new RepositoryContext(mongoUri, primarySites, identifierClient, tcgaClient);
  }

  private static IdentifierClient createIdentifierClient() {
    // TODO: Externalize
    return new HashIdentifierClient();
  }

  private static TCGAClient createTCGAClient() {
    // TODO: Externalize
    return new TCGAClient();
  }

  private static void logBanner(String message) {
    log.info(repeat("-", 80));
    log.info(message);
    log.info(repeat("-", 80));
  }

}
