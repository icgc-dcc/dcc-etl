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
package org.icgc.dcc.etl.db.importer.repo.pcawg;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.URLs.getUrl;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_ARCHIVE_BASE_URL;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.core.id.IdentifierClient;
import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;
import org.icgc.dcc.etl.db.importer.repo.pcawg.core.PCAWGProcessor;
import org.icgc.dcc.etl.db.importer.repo.pcawg.reader.PCAWGDonorArchiveReader;
import org.icgc.dcc.etl.db.importer.repo.pcawg.writer.PCAWGFileWriter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoClientURI;

@Slf4j
@RequiredArgsConstructor
public class PCAWGImporter {

  /**
   * JSONL (new line delimited JSON file) dump of clean, curated PCAWG donor information.
   * 
   * @see http://jsonlines.org/
   */
  private static final URL DEFAULT_PCAWG_DONOR_ARCHIVE_URL =
      getUrl(PCAWG_ARCHIVE_BASE_URL + "/santa_cruz_pilot/santa_cruz_pilot.v1.2015_0425.jsonl");

  /**
   * Configuration.
   */
  @NonNull
  private final MongoClientURI mongoUri;
  @NonNull
  private final URL archiveUrl;

  /**
   * Metadata.
   */
  @NonNull
  private final Map<String, String> primarySites;

  /**
   * Dependencies.
   */
  @NonNull
  private final IdentifierClient identifierClient;

  public PCAWGImporter(@NonNull MongoClientURI mongoUri, Map<String, String> primarySites,
      @NonNull IdentifierClient identifierClient) {
    this.mongoUri = mongoUri;
    this.archiveUrl = DEFAULT_PCAWG_DONOR_ARCHIVE_URL;
    this.primarySites = primarySites;
    this.identifierClient = identifierClient;
  }

  @SneakyThrows
  public void execute() {
    val watch = createStarted();

    log.info("Reading donors...");
    val donors = readDonors();
    log.info("Finished reading {} donors", formatCount(donors));

    log.info("Processing donor files...");
    val files = processFiles(donors);
    log.info("Finished processing {} donor files", formatCount(files));

    log.info("Writing files...");
    writeFiles(files);
    log.info("Finished writing {} donor files", formatCount(files));

    log.info("Imported {} files in {}.", formatCount(files), watch);
  }

  private Iterable<ObjectNode> readDonors() throws IOException {
    val reader = new PCAWGDonorArchiveReader(archiveUrl);
    return reader.readDonors();
  }

  private Iterable<RepositoryFile> processFiles(Iterable<ObjectNode> donors) {
    val processor = new PCAWGProcessor(primarySites);
    return processor.processFiles(donors);
  }

  private void writeFiles(Iterable<RepositoryFile> files) throws IOException {
    @Cleanup
    val writer = new PCAWGFileWriter(mongoUri);
    writer.write(files);
  }

}
