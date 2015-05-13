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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.collect.Iterables.contains;
import static org.apache.commons.lang.StringUtils.repeat;

import java.util.Collection;
import java.util.List;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.repo.cghub.CGHubImporter;
import org.icgc.dcc.etl.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.repo.core.RepositoryFileIndexer;
import org.icgc.dcc.etl.repo.core.RepositorySourceFileImporter;
import org.icgc.dcc.etl.repo.model.RepositorySource;
import org.icgc.dcc.etl.repo.pcawg.PCAWGImporter;
import org.icgc.dcc.etl.repo.tcga.TCGAImporter;

import com.google.common.collect.ImmutableList;

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
   * Dependencies.
   */
  @NonNull
  private final RepositoryFileContext context;

  public void execute() {
    execute(RepositorySource.values());
  }

  @NonNull
  public void execute(RepositorySource... activeSources) {
    execute(ImmutableList.copyOf(activeSources));
  }

  @NonNull
  public void execute(Iterable<RepositorySource> activeSources) {
    val watch = createStarted();

    // The business
    val exceptions = ImmutableList.builder()
        .addAll(writeFiles(activeSources))
        .addAll(indexFiles())
        .build();

    log.info("Finished importing repository in {}", watch);
    checkState(exceptions.isEmpty(), "Exception(s) processing %s", exceptions);
  }

  private Collection<Exception> writeFiles(Iterable<RepositorySource> activeSources) {
    val importers = createImporters(context);

    val exceptions = ImmutableList.<Exception> builder();
    for (val importer : importers) {
      boolean active = contains(activeSources, importer.getSource());
      if (active) {
        try {
          logBanner("Importing '" + importer.getSource() + "' sourced files");
          importer.execute();
        } catch (Exception e) {
          log.error("Error procesing '" + importer.getSource() + "': ", e);
          exceptions.add(e);
        }
      }
    }

    return exceptions.build();
  }

  @SneakyThrows
  private Iterable<Exception> indexFiles() {
    val exceptions = ImmutableList.<Exception> builder();
    try {
      logBanner("Indexing files");
      @Cleanup
      val indexer = new RepositoryFileIndexer(context.getMongoUri(), context.getEsUri());
      indexer.indexFiles();
    } catch (Exception e) {
      exceptions.add(e);
    }

    return exceptions.build();
  }

  private static List<RepositorySourceFileImporter> createImporters(RepositoryFileContext context) {
    return ImmutableList.of(
        new PCAWGImporter(context),
        new TCGAImporter(context),
        new CGHubImporter(context));
  }

  private static void logBanner(String message) {
    log.info(repeat("-", 80));
    log.info(message);
    log.info(repeat("-", 80));
  }

}
