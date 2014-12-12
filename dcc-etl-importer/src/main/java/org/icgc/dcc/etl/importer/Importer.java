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
package org.icgc.dcc.etl.importer;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.icgc.dcc.common.core.util.Splitters.COMMA;
import static org.icgc.dcc.etl.importer.gene.util.InGeneFilter.in;
import static org.icgc.dcc.etl.importer.util.Importers.getRemoteCgsUri;
import static org.icgc.dcc.etl.importer.util.Importers.getRemoteGenesBsonUri;

import java.io.File;
import java.util.Collections;
import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.client.api.ICGCClientConfig;
import org.icgc.dcc.etl.importer.cgc.CgcImporter;
import org.icgc.dcc.etl.importer.fathmm.FathmmImporter;
import org.icgc.dcc.etl.importer.fi.FunctionalImpactImporter;
import org.icgc.dcc.etl.importer.gene.GeneImporter;
import org.icgc.dcc.etl.importer.go.GoImporter;
import org.icgc.dcc.etl.importer.pathway.PathwayImporter;
import org.icgc.dcc.etl.importer.project.ProjectImporter;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.mongodb.MongoClientURI;

@Slf4j
@RequiredArgsConstructor
public class Importer {

  /**
   * Constants.
   */
  public static final String INCLUDED_GENE_IDS_SYSTEM_PROPERTY_NAME = Importer.class + ".geneIds";

  /**
   * Configuration
   */
  @NonNull
  private final String releaseMongoUri;
  @NonNull
  private final File cacheDir;
  @NonNull
  private final String fathmmPostgresqlUri;
  @NonNull
  private final ICGCClientConfig icgcConfig;

  public void import_(@NonNull String releaseName, @NonNull Set<String> projectKeys) {
    log.info("Release: {} ", releaseName);
    log.info("Projects: {} ", projectKeys);

    val releaseUri = new MongoClientURI(releaseUri(releaseName));
    val watch = Stopwatch.createStarted();

    log.info("Importing projects...");
    importProjects(icgcConfig, releaseUri, projectKeys);
    log.info("Finished importing projects in {} ...", watch);

    watch.reset().start();
    log.info("Importing genes...");
    importGenes(releaseUri);
    log.info("Finished importing genes in {} ...", watch);

    watch.reset().start();
    log.info("Importing CGC...");
    importCgc(releaseUri);
    log.info("Finished importing CGC in {} ...", watch);

    watch.reset().start();
    log.info("Importing pathways...");
    importPathways(releaseUri);
    log.info("Finished importing pathways in {} ...", watch);

    watch.reset().start();
    log.info("Importing GO...");
    importGo(releaseUri);
    log.info("Finished importing GO in {} ...", watch);

    watch.reset().start();
    log.info("Importing Fathmm predictions...");
    importFathmmPredictions(fathmmPostgresqlUri, releaseUri);
    log.info("Finished importing Fathmm predictions {} ...", watch);

    watch.reset().start();
    log.info("Importing functional impact predictions");
    importFunctionalImpact(releaseUri);
    log.info("Finished importing functional impact predictions {} ...", watch);
  }

  private void importProjects(ICGCClientConfig config, MongoClientURI releaseUri, Set<String> projectKeys) {
    val projectImporter = new ProjectImporter(config, fromNullable(projectKeys), releaseUri);
    projectImporter.execute();
  }

  private void importGenes(MongoClientURI releaseUri) {
    val geneImporter = new GeneImporter(releaseUri, getRemoteGenesBsonUri());

    val includedGeneIds = getIncludedGeneIds();
    val all = includedGeneIds.isEmpty();
    if (all) {
      geneImporter.execute();
    } else {
      log.warn("*** Only importing the following genes: {}", includedGeneIds);
      geneImporter.execute(in(includedGeneIds));
    }
  }

  private void importCgc(MongoClientURI releaseUri) {
    val cgcImporter = new CgcImporter(releaseUri, getRemoteCgsUri());
    cgcImporter.execute();
  }

  private void importPathways(MongoClientURI releaseUri) {
    val pathwayImporter = new PathwayImporter(releaseUri);
    pathwayImporter.execute();
  }

  private void importGo(MongoClientURI releaseUri) {
    val goImporter = new GoImporter(releaseUri);
    goImporter.execute();
  }

  private void importFunctionalImpact(MongoClientURI releaseUri) {
    val fiImporter = new FunctionalImpactImporter(releaseUri);
    fiImporter.execute();
  }

  private void importFathmmPredictions(String fathmmPostgresqlUri, MongoClientURI releaseUri) {
    val fathmmImporter = new FathmmImporter(fathmmPostgresqlUri, releaseUri);
    fathmmImporter.execute();
  }

  private String releaseUri(String releaseName) {
    return releaseMongoUri + (releaseMongoUri.charAt(releaseMongoUri.length() - 1) == '/' ? "" : "/") + releaseName;
  }

  private static Set<String> getIncludedGeneIds() {
    val geneIds = System.getProperty(INCLUDED_GENE_IDS_SYSTEM_PROPERTY_NAME);
    if (isNullOrEmpty(geneIds)) {
      // All
      return Collections.emptySet();
    } else {
      // Subset
      return ImmutableSet.copyOf(COMMA.split(geneIds));
    }
  }

}
