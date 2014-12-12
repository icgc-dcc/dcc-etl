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
package org.icgc.dcc.etl.importer.gene;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.etl.importer.gene.util.AllGeneFilter.all;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.gene.core.GeneFilter;
import org.icgc.dcc.etl.importer.gene.writer.GeneWriter;

import com.mongodb.MongoClientURI;

@Slf4j
@RequiredArgsConstructor
public class GeneImporter {

  @NonNull
  private final URI genesBsonUri;
  @NonNull
  private final MongoClientURI mongoUri;

  public GeneImporter(@NonNull MongoClientURI mongoUri, @NonNull URI genesBsonUri) {
    this.genesBsonUri = genesBsonUri;
    this.mongoUri = mongoUri;
  }

  public void execute() {
    execute(all());
  }

  @SneakyThrows
  public void execute(GeneFilter geneFilter) {
    val watch = createStarted();

    log.info("Reading genes BSON stream from {}...", genesBsonUri);
    val genesBson = readGenesBson();

    log.info("Writing genes to {}...", mongoUri);
    writeGenes(genesBson, geneFilter);

    log.info("Finished writing genes iin {}", watch);
  }

  private InputStream readGenesBson() throws IOException {
    return genesBsonUri.toURL().openStream();
  }

  private void writeGenes(InputStream genesBson, GeneFilter geneFilter) throws IOException {
    @Cleanup
    val writer = new GeneWriter(mongoUri);
    writer.write(genesBson, geneFilter);
  }

}
