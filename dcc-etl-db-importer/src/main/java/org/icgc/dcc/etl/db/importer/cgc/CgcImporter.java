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
package org.icgc.dcc.etl.db.importer.cgc;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.cgc.reader.CgcReader;
import org.icgc.dcc.etl.db.importer.cgc.writer.CgcWriter;

import com.mongodb.MongoClientURI;

/**
 * "Cancer Gene Census" gene list importer.
 */
@Slf4j
@RequiredArgsConstructor
public class CgcImporter {

  /**
   * Configuration.
   */
  @NonNull
  private final URI cgsUri;
  @NonNull
  private final MongoClientURI mongoUri;

  public CgcImporter(@NonNull MongoClientURI mongoUri, URI cgsUri) {
    this.cgsUri = cgsUri;
    this.mongoUri = mongoUri;
  }

  @SneakyThrows
  public void execute() {
    val watch = createStarted();

    log.info("Reading CGC from {}...", cgsUri);
    val cgc = readCgc();

    log.info("Writing CGC to... {}", mongoUri);
    writeCgc(cgc);

    log.info("Finished writing CGC with {} genes in {}", formatCount(cgc), watch);
  }

  public Iterable<Map<String, String>> readCgc() throws IOException {
    return new CgcReader(cgsUri).read();
  }

  private void writeCgc(Iterable<Map<String, String>> cgc) throws IOException {
    @Cleanup
    val writer = new CgcWriter(mongoUri);
    writer.write(cgc);
  }

}
