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
package org.icgc.dcc.etl.db.importer.diagram;

import static com.google.common.base.Stopwatch.createStarted;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.diagram.model.DiagramModel;
import org.icgc.dcc.etl.db.importer.diagram.reader.DiagramReader;
import org.icgc.dcc.etl.db.importer.diagram.writer.DiagramWriter;

import com.google.common.base.Strings;
import com.mongodb.MongoClientURI;

/**
 * Imports all the Reactome Pathway Diagram Data
 * <ol>
 * <li>Gets list of pathway diagrams: diagrammed and non-diagrammed</li>
 * 
 * <li>For each diagrammed pathway, add a diagram:</li>
 * <ul>
 * <li>Diagram: xml <code>/pathwayDiagram</code></li>
 * <li>ProteinMap: uniprot id to reactome db id <code>/getPhysicalToReferenceEntityMaps</code></li>
 * <li>Highlights: --</li>
 * </ul>
 * <li>For each nondiagrammed pathway, add a diagram:</li>
 * <ul>
 * <li>Diagram: <i>from diagrammed parent</i></li>
 * <li>ProteinMap: <i>from diagrammed parent</i></li>
 * <li>Highlights: highlights <code>/getContainedEventIds</code></li>
 * </ul>
 * <li>Get a REACT id for each diagram <code>/queryById</code></li>
 * </ol>
 */
@Slf4j
@RequiredArgsConstructor
public class DiagramImporter {

  @NonNull
  private final MongoClientURI mongoUri;

  public final static String INCLUDED_REACTOME_DIAGRAMS = DiagramImporter.class + ".diagramIds";

  @SneakyThrows
  public void execute() {
    val watch = createStarted();

    log.info("Reading diagram model...");
    val model = readDiagramModel();

    log.info("Writing diagram model to {}...", mongoUri);
    writeDiagramModel(model);

    log.info("Finished importing diagrams in {}", watch);
  }

  private DiagramModel readDiagramModel() throws Exception {
    val diagramIds = System.getProperty(INCLUDED_REACTOME_DIAGRAMS);

    if (Strings.isNullOrEmpty(diagramIds)) {
      return new DiagramReader().read(new ArrayList<String>());
    } else {
      return new DiagramReader().read(Arrays.asList(diagramIds.split(",")));
    }
  }

  private void writeDiagramModel(DiagramModel model) throws UnknownHostException, IOException {
    @Cleanup
    val writer = new DiagramWriter(mongoUri);
    writer.write(model);
  }

}
