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

import static org.elasticsearch.common.collect.Maps.uniqueIndex;
import static org.icgc.dcc.etl.db.importer.util.Importers.getRemoteCgsUri;
import static org.icgc.dcc.etl.db.importer.util.Importers.getRemoteGenesBsonUri;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.client.api.ICGCClientConfig;
import org.icgc.dcc.etl.db.importer.cgc.CgcImporter;
import org.icgc.dcc.etl.db.importer.cli.CollectionName;
import org.icgc.dcc.etl.db.importer.core.Importer;
import org.icgc.dcc.etl.db.importer.diagram.DiagramImporter;
import org.icgc.dcc.etl.db.importer.gene.GeneImporter;
import org.icgc.dcc.etl.db.importer.go.GoImporter;
import org.icgc.dcc.etl.db.importer.pathway.PathwayImporter;
import org.icgc.dcc.etl.db.importer.project.ProjectImporter;
import org.icgc.dcc.etl.db.importer.repo.RepositoryImporter;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientURI;

@Slf4j
public class DBImporter {

  /**
   * Processing order.
   */
  private static List<CollectionName> COLLECTION_ORDER = ImmutableList.of(
      CollectionName.PROJECTS,
      CollectionName.GENES,
      CollectionName.CGC,
      CollectionName.PATHWAYS,
      CollectionName.GO,
      CollectionName.DIAGRAMS,
      CollectionName.FILES
      );

  /**
   * Configuration
   */
  @NonNull
  private final MongoClientURI mongoUri;
  @NonNull
  private final String esUri = "es://localhost:9300";
  @NonNull
  private final ICGCClientConfig icgcConfig;
  @NonNull
  private final Map<CollectionName, Importer> importers;

  public DBImporter(@NonNull String mongoUri, @NonNull ICGCClientConfig icgcConfig) {
    this.mongoUri = new MongoClientURI(mongoUri);
    this.icgcConfig = icgcConfig;
    this.importers = createImporters();
  }

  public void execute(@NonNull Collection<CollectionName> collectionNames) {
    for (val collectionName : COLLECTION_ORDER) {
      if (collectionNames.contains(collectionName)) {
        val importer = importers.get(collectionName);

        val watch = Stopwatch.createStarted();
        log.info("Importing '{}'...", collectionName);
        importer.execute();
        log.info("Finished importing '{}' in {}", collectionName, watch);
      }
    }
  }

  private Map<CollectionName, Importer> createImporters() {
    val importers = ImmutableList.<Importer> of(
        new ProjectImporter(icgcConfig, mongoUri),
        new GeneImporter(mongoUri, getRemoteGenesBsonUri()),
        new CgcImporter(mongoUri, getRemoteCgsUri()),
        new PathwayImporter(mongoUri),
        new GoImporter(mongoUri),
        new DiagramImporter(mongoUri),
        new RepositoryImporter(mongoUri, esUri)
        );

    return uniqueIndex(importers, (Importer importer) -> importer.getCollectionName());
  }

}
