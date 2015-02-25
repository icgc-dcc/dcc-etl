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
package org.icgc.dcc.etl.db.importer.pathway;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.etl.db.importer.util.Importers.getRemoteReactomeHierarchyUri;
import static org.icgc.dcc.etl.db.importer.util.Importers.getRemoteReactomeSummationUri;
import static org.icgc.dcc.etl.db.importer.util.Importers.getRemoteReactomeUniprotUri;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.pathway.core.PathwayModel;
import org.icgc.dcc.etl.db.importer.pathway.reader.PathwayModelReader;
import org.icgc.dcc.etl.db.importer.pathway.writer.PathwayWriter;

import com.mongodb.MongoClientURI;

/**
 * Create Pathway collection in mongodb, and subsequently embed pathway information into the gene-collection
 */
@Slf4j
@RequiredArgsConstructor
public class PathwayImporter {

  /**
   * Configuration.
   */
  @NonNull
  private final URI uniprotFile;
  @NonNull
  private final URI summationFile;
  @NonNull
  private final URI hierarchyFile;
  @NonNull
  private final MongoClientURI mongoUri;

  public PathwayImporter(@NonNull MongoClientURI mongoUri) {
    this.uniprotFile = getRemoteReactomeUniprotUri();
    this.summationFile = getRemoteReactomeSummationUri();
    this.hierarchyFile = getRemoteReactomeHierarchyUri();
    this.mongoUri = mongoUri;
  }

  @SneakyThrows
  public void execute() {
    val watch = createStarted();

    log.info("Reading pathway model...");
    val model = readPathwayModel(uniprotFile, summationFile, hierarchyFile);

    log.info("Writing pathway model to {}...", mongoUri);
    writePathwayModel(model);

    log.info("Finished importing pathways in {}", watch);
  }

  private PathwayModel readPathwayModel(URI uniprotFile, URI summationFile, URI hierarchyFile) throws IOException {
    return new PathwayModelReader().read(uniprotFile, summationFile, hierarchyFile);
  }

  private void writePathwayModel(PathwayModel model) throws UnknownHostException, IOException {
    @Cleanup
    val writer = new PathwayWriter(mongoUri);
    writer.write(model);
  }

}
