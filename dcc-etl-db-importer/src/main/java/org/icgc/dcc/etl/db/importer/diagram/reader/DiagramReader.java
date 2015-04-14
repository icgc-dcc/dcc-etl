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
package org.icgc.dcc.etl.db.importer.diagram.reader;

import java.util.List;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.diagram.model.Diagram;
import org.icgc.dcc.etl.db.importer.diagram.model.DiagramModel;
import org.icgc.dcc.etl.db.importer.diagram.model.Pathways;

import com.google.common.collect.ImmutableSet;

@Slf4j
public class DiagramReader {

  public final static String REACTOME_BASE_URL = "http://reactomews.oicr.on.ca:8080/ReactomeRESTfulAPI/RESTfulWS/";
  public final static String NOT_HIGHLIGHTED = "";

  public DiagramModel read(@NonNull List<String> testPathways) throws Exception {
    val model = new DiagramModel();
    val listReader = new DiagramListReader();

    Pathways pathways;
    if (testPathways.isEmpty()) {
      log.info("Reading list of pathways..");
      pathways = listReader.readPathwayList();
    } else {
      log.info("Using given test pathway list {}...", testPathways);
      pathways = new Pathways(ImmutableSet.copyOf(testPathways), new ImmutableSet.Builder<String>().build());
    }

    int count = 1;

    log.info("Getting all diagrammed pathways and their protein maps...");
    for (val pathwayId : pathways.getDiagrammed()) {
      val diagram = new Diagram();

      diagram.setDiagram(new DiagramXmlReader().readPathwayXml(pathwayId));
      diagram.setProteinMap(new DiagramProteinMapReader().readProteinMap(pathwayId));
      diagram.setHighlights(NOT_HIGHLIGHTED);

      model.addDiagram(pathwayId, diagram);

      log.info("[{}/{}] Added diagram '{}'", pathwayId, count, pathways.getDiagrammed().size());

      Thread.sleep(3000); // To avoid crashing reactome's servers
      count++;
    }

    count = 1;

    log.info("Adding all non-diagrammed pathways and their highlights...");
    for (val id : pathways.getNotDiagrammed()) {
      val diagrammedId = parseDiagramId(id);
      val nonDiagrammedId = parseNonDiagramId(id);

      val baseDiagram = model.getDiagrams().get(diagrammedId);
      baseDiagram.setHighlights(new DiagramHighlightReader().readHighlights(nonDiagrammedId));

      model.addDiagram(nonDiagrammedId, baseDiagram);

      log.info("[{}/{}] Added non-diagram '{}'", id, count, pathways.getNotDiagrammed().size());

      Thread.sleep(1000);
      count++;
    }

    val updatedModel = new DiagramModel();

    log.info("Replacing all dbIds with REACT ids...");
    for (val entry : model.getDiagrams().entrySet()) {
      val reactId = listReader.getReactId(entry.getKey());
      updatedModel.addDiagram(reactId, entry.getValue());
      log.info("Saved diagram  dbId '{}' as {}", entry.getKey(), reactId);
    }

    return updatedModel;
  }

  private String parseDiagramId(String id) {
    return id.substring(id.indexOf("-") + 1);
  }

  private String parseNonDiagramId(String id) {
    return id.substring(0, id.indexOf("-"));
  }

}
