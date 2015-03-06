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

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.ReleaseCollection.CGC_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GO_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PATHWAY_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.common.core.util.Splitters.COMMA;

import java.io.File;
import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.client.api.ICGCClientConfig;
import org.icgc.dcc.etl.importer.fathmm.FathmmImporter;
import org.icgc.dcc.etl.importer.fi.FunctionalImpactImporter;
import org.icgc.dcc.etl.importer.util.CollectionImporter;
import org.icgc.dcc.etl.importer.util.DocumentFilter;

import com.google.common.base.Optional;
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
  private final String geneMongoUri;
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
    val projectKeysWrapper = DocumentFilter.builder().idField(PROJECT_ID).values(projectKeys).build();
    val projectCollectionImporter =
        new CollectionImporter(new MongoClientURI(geneMongoUri), releaseUri, Optional.of(projectKeysWrapper),
            Optional.<DocumentFilter> absent());
    projectCollectionImporter.import_(PROJECT_COLLECTION.getId());
    log.info("Finished importing projects in {} ...", watch);

    watch.reset().start();
    log.info("Importing genes...");
    val includedGeneIds = getIncludedGeneIds();
    val geneCollectionImporter =
        new CollectionImporter(new MongoClientURI(geneMongoUri), releaseUri, includedGeneIds,
            Optional.<DocumentFilter> absent());
    geneCollectionImporter.import_(GENE_COLLECTION.getId());
    log.info("Finished importing genes in {} ...", watch);

    val collectionImporter =
        new CollectionImporter(new MongoClientURI(geneMongoUri), releaseUri, Optional.<DocumentFilter> absent(),
            Optional.<DocumentFilter> absent());

    watch.reset().start();
    log.info("Importing CGC...");
    collectionImporter.import_(CGC_COLLECTION.getId());
    log.info("Finished importing CGC in {} ...", watch);

    watch.reset().start();
    log.info("Importing pathways...");
    collectionImporter.import_(PATHWAY_COLLECTION.getId());
    log.info("Finished importing pathways in {} ...", watch);

    watch.reset().start();
    log.info("Importing GO...");
    collectionImporter.import_(GO_COLLECTION.getId());
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

  private static Optional<DocumentFilter> getIncludedGeneIds() {
    val geneIds = System.getProperty(INCLUDED_GENE_IDS_SYSTEM_PROPERTY_NAME);
    if (isNullOrEmpty(geneIds)) {
      // All
      return Optional.<DocumentFilter> absent();
    } else {
      // Subset
      val ids = ImmutableSet.copyOf(COMMA.split(geneIds));
      return Optional.of(DocumentFilter.builder().idField(GENE_ID).values(ids).build());
    }
  }
}
