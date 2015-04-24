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
package org.icgc.dcc.etl.db.importer.pcawg;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.URLs.getUrl;

import java.io.IOException;
import java.net.URL;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.pcawg.core.PCAWGDonorFileConverter;
import org.icgc.dcc.etl.db.importer.pcawg.reader.PCAWGReader;
import org.icgc.dcc.etl.db.importer.pcawg.writer.PCAWGWriter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientURI;

@Slf4j
@RequiredArgsConstructor
public class PCAWGImporter {

  /**
   * JSONL (new line delimited JSON file) dump of clean, curated PCAWG donor information.
   */
  private static final URL PCAWG_DONOR_ARCHIVE_URL =
      getUrl("http://pancancer.info/gnos_metadata/2015-04-24_02-02-57_UTC/ucsc_polit_donor_r_150424020257.jsonl.gz");

  /**
   * Configuration
   */
  @NonNull
  private final MongoClientURI mongoUri;

  @SneakyThrows
  public void execute() {
    val watch = createStarted();

    log.info("Reading donors...");
    val donors = readDonors();

    log.info("Converting donors...");
    val files = convert(donors);

    log.info("Writing files...");
    writeFiles(files);

    log.info("Imported {} files in {}.", formatCount(files), watch);
  }

  private Iterable<ObjectNode> readDonors() throws IOException {
    val reader = new PCAWGReader(PCAWG_DONOR_ARCHIVE_URL);
    return reader.read();
  }

  private Iterable<ObjectNode> convert(Iterable<ObjectNode> donors) {
    val converter = new PCAWGDonorFileConverter();

    val files = ImmutableList.<ObjectNode> builder();
    for (val donor : donors) {
      val donorFiles = converter.convert(donor);

      files.addAll(donorFiles);
    }

    return files.build();
  }

  private void writeFiles(Iterable<ObjectNode> files) throws IOException {
    @Cleanup
    val writer = new PCAWGWriter(mongoUri);
    writer.write(files);
  }

}
