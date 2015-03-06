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

import static org.icgc.dcc.etl.db.importer.util.Importers.getRemoteCgsUri;
import static org.icgc.dcc.etl.db.importer.util.Importers.getRemoteGenesBsonUri;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.client.api.ICGCClientConfig;
import org.icgc.dcc.etl.db.importer.cgc.CgcImporter;
import org.icgc.dcc.etl.db.importer.cli.CollectionName;
import org.icgc.dcc.etl.db.importer.gene.GeneImporter;
import org.icgc.dcc.etl.db.importer.go.GoImporter;
import org.icgc.dcc.etl.db.importer.pathway.PathwayImporter;
import org.icgc.dcc.etl.db.importer.project.ProjectImporter;

import com.google.common.base.Stopwatch;
import com.mongodb.MongoClientURI;

@Slf4j
@RequiredArgsConstructor
public class DBImporter {

  /**
   * Configuration
   */
  @NonNull
  private final String geneMongoUri;
  @NonNull
  private final ICGCClientConfig icgcConfig;

  public void import_(List<CollectionName> collections) {

    val watch = Stopwatch.createStarted();
    val mongoUri = new MongoClientURI(geneMongoUri);

    if (collections.contains(CollectionName.PROJECTS)) {
      log.info("Importing projects...");
      importProjects(icgcConfig, mongoUri);
      log.info("Finished importing projects in {} ...", watch);
    }

    if (collections.contains(CollectionName.GENES)) {
      watch.reset().start();
      log.info("Importing genes...");
      importGenes(mongoUri);
      log.info("Finished importing genes in {} ...", watch);
    }

    if (collections.contains(CollectionName.CGC)) {
      watch.reset().start();
      log.info("Importing CGC...");
      importCgc(mongoUri);
      log.info("Finished importing CGC in {} ...", watch);
    }

    if (collections.contains(CollectionName.PATHWAYS)) {
      watch.reset().start();
      log.info("Importing pathways...");
      importPathways(mongoUri);
      log.info("Finished importing pathways in {} ...", watch);
    }

    if (collections.contains(CollectionName.GO)) {
      watch.reset().start();
      log.info("Importing GO...");
      importGo(mongoUri);
      log.info("Finished importing GO in {} ...", watch);
    }

  }

  private static void importProjects(ICGCClientConfig config, MongoClientURI releaseUri) {
    val projectImporter = new ProjectImporter(config, releaseUri);
    projectImporter.execute();
  }

  private static void importGenes(MongoClientURI releaseUri) {
    val geneImporter = new GeneImporter(releaseUri, getRemoteGenesBsonUri());
    geneImporter.execute();
  }

  private static void importCgc(MongoClientURI releaseUri) {
    val cgcImporter = new CgcImporter(releaseUri, getRemoteCgsUri());
    cgcImporter.execute();
  }

  private static void importPathways(MongoClientURI releaseUri) {
    val pathwayImporter = new PathwayImporter(releaseUri);
    pathwayImporter.execute();
  }

  private static void importGo(MongoClientURI releaseUri) {
    val goImporter = new GoImporter(releaseUri);
    goImporter.execute();
  }

}
