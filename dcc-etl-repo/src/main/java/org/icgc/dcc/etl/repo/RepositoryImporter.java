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
package org.icgc.dcc.etl.repo;

import static com.google.common.base.Stopwatch.createStarted;
import static org.apache.commons.lang.StringUtils.repeat;

import java.util.List;
import java.util.Map;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.tcga.TCGAClient;
import org.icgc.dcc.etl.core.id.HttpIdentifierClient;
import org.icgc.dcc.etl.core.id.IdentifierClient;
import org.icgc.dcc.etl.repo.cghub.CGHubImporter;
import org.icgc.dcc.etl.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.repo.core.RepositoryFileIndexer;
import org.icgc.dcc.etl.repo.core.RepositoryProjectReader;
import org.icgc.dcc.etl.repo.core.RepositorySourceFileImporter;
import org.icgc.dcc.etl.repo.model.RepositorySource;
import org.icgc.dcc.etl.repo.pcawg.PCAWGImporter;
import org.icgc.dcc.etl.repo.tcga.TCGAImporter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.mongodb.MongoClientURI;

/**
 * Importer for the ICGC "Data Repository" feature which imports file metadata from various external data sources.
 * 
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/JSON+structure+for+ICGC+data+repository
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/UI+-+The+new+ICGC+DCC+data+repository+-+Simplified+version+Phase+1
 */
@Slf4j
@RequiredArgsConstructor
public class RepositoryImporter {

  /**
   * Configuration
   */
  @NonNull
  private final MongoClientURI geneMongoUri;
  @NonNull
  private final MongoClientURI repoMongoUri;
  @NonNull
  private final String esUri;

  public void execute() {
    execute(RepositorySource.values());
  }

  @NonNull
  public void execute(RepositorySource... sources) {
    execute(ImmutableList.copyOf(sources));
  }

  @NonNull
  public void execute(Iterable<RepositorySource> sources) {
    val watch = createStarted();

    // The business
    writeFiles(sources);
    indexFiles();

    log.info("Finished importing repository in {}", watch);
  }

  private void writeFiles(Iterable<RepositorySource> sources) {
    val context = createRepositoryContext();
    val importers = createImporters(context);

    for (val importer : importers) {
      if (Iterables.contains(sources, importer.getSource())) {
        logBanner("Importing " + importer.getSource() + " files");
        importer.execute();
      }
    }
  }

  @SneakyThrows
  private void indexFiles() {
    logBanner("Indexing files");
    @Cleanup
    val indexer = new RepositoryFileIndexer(repoMongoUri, esUri);
    indexer.indexFiles();
  }

  private List<RepositorySourceFileImporter> createImporters(RepositoryFileContext context) {
    return ImmutableList.of(
        new CGHubImporter(context),
        new PCAWGImporter(context),
        new TCGAImporter(context)
        );

  }

  private RepositoryFileContext createRepositoryContext() {
    // TODO: Externalize
    val primarySites = getProjectPrimarySites();
    val identifierClient = createIdentifierClient();
    val tcgaClient = createTCGAClient();

    return new RepositoryFileContext(repoMongoUri, primarySites, identifierClient, tcgaClient);
  }

  private static IdentifierClient createIdentifierClient() {
    // TODO: Externalize
    val dummyRelease = "";
    return new HttpIdentifierClient("http://hcache-dcc.oicr.on.ca:5391/", dummyRelease);
  }

  private static TCGAClient createTCGAClient() {
    // TODO: Externalize
    return new TCGAClient();
  }

  @SneakyThrows
  private Map<String, String> getProjectPrimarySites() {
    // TODO: Externalize
    @Cleanup
    val projectReader = new RepositoryProjectReader(geneMongoUri);
    return projectReader.getPrimarySites();
  }

  private static void logBanner(String message) {
    log.info(repeat("-", 80));
    log.info(message);
    log.info(repeat("-", 80));
  }

}
