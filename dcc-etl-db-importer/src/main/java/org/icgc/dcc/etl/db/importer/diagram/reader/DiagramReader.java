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

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.icgc.dcc.etl.db.importer.diagram.reader.DiagramHighlightReader.CONTAINED_EVENTS_URL;
import static org.icgc.dcc.etl.db.importer.diagram.reader.DiagramHighlightReader.getFailedPathways;

import java.io.IOException;
import java.util.List;

import javax.xml.transform.TransformerException;

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
  private final static int PATHWAY_ENDPOINT_DELAY_MILLISECONDS = 3000;
  private final static int ID_ENDPOINT_DELAY_MILLISECONDS = 1000;

  private DiagramListReader listReader = new DiagramListReader();

  public DiagramModel read(@NonNull List<String> testPathways) throws Exception {
    val model = new DiagramModel();

    val pathways = resolvePathways(testPathways);

    printEstimateTime(pathways.getDiagrammed().size(), pathways.getNotDiagrammed().size());

    int count = 1;

    log.info("Getting all diagrammed pathways and their protein maps...");
    for (val pathwayId : pathways.getDiagrammed()) {
      val diagram = new Diagram();

      val xmlReader = new DiagramXmlReader();
      val proteinMapReader = new DiagramProteinMapReader();

      diagram.setDiagram(xmlReader.readPathwayXml(pathwayId));
      diagram.setProteinMap(proteinMapReader.readProteinMap(pathwayId));
      diagram.setHighlights(NOT_HIGHLIGHTED);

      model.addDiagram(pathwayId, diagram);

      log.info("[{}/{}] Added diagram '{}'", count, pathways.getDiagrammed().size(), pathwayId);

      Thread.sleep(PATHWAY_ENDPOINT_DELAY_MILLISECONDS); // To avoid crashing reactome's servers
      count++;
    }

    count = 1;

    log.info("Adding all non-diagrammed pathways and their highlights...");
    for (val id : pathways.getNotDiagrammed()) {
      val diagrammedId = parseDiagramId(id);
      val nonDiagrammedId = parseNonDiagramId(id);

      val highlightReader = new DiagramHighlightReader();

      val baseDiagram = model.getDiagrams().get(diagrammedId);
      val newDiagram = new Diagram();
      newDiagram.setHighlights(highlightReader.readHighlights(nonDiagrammedId));
      newDiagram.setDiagram(baseDiagram.getDiagram());
      newDiagram.setProteinMap(baseDiagram.getProteinMap());

      model.addDiagram(nonDiagrammedId, newDiagram);

      log.info("[{}/{}] Added non-diagram '{}' of pathway '{}'", count, pathways.getNotDiagrammed().size(),
          nonDiagrammedId, diagrammedId);

      Thread.sleep(PATHWAY_ENDPOINT_DELAY_MILLISECONDS);
      count++;
    }

    log.info("Failed to read highlights of {} pathways...", getFailedPathways().size());
    printFailed();

    val updatedModel = new DiagramModel();

    log.info("Replacing all dbIds with REACT ids...");
    for (val entry : model.getDiagrams().entrySet()) {
      val reactId = listReader.getReactId(entry.getKey());
      updatedModel.addDiagram(reactId, entry.getValue());

      Thread.sleep(ID_ENDPOINT_DELAY_MILLISECONDS);
      log.info("Saved diagram  dbId '{}' as {}", entry.getKey(), reactId);
    }

    return updatedModel;
  }

  private void printEstimateTime(int diagrammed, int nonDiagrammed) {
    int pathwayDelay = (diagrammed + nonDiagrammed) * PATHWAY_ENDPOINT_DELAY_MILLISECONDS;
    int idDelay = (diagrammed + nonDiagrammed) * ID_ENDPOINT_DELAY_MILLISECONDS;
    int totalTime = pathwayDelay + idDelay + diagrammed * 300 + nonDiagrammed * 150;

    log.info(format("Estimated Time: %d min, %d sec",
        MILLISECONDS.toMinutes(totalTime),
        MILLISECONDS.toSeconds(totalTime) - MINUTES.toSeconds(MILLISECONDS.toMinutes(totalTime))
        ));
  }

  private void printFailed() {
    getFailedPathways().forEach(
        pathwayId -> log.info("Couldn't read highlights of pathway '{}'"
            + "\nReactome URL:{}", pathwayId, format(CONTAINED_EVENTS_URL, pathwayId)));
  }

  private Pathways resolvePathways(List<String> testPathways) throws IOException, TransformerException {
    if (testPathways.isEmpty()) {
      log.info("Reading list of pathways..");
      return listReader.readPathwayList();
    } else {
      log.info("Using given test pathway list {}...", testPathways);
      return new Pathways(ImmutableSet.copyOf(testPathways), new ImmutableSet.Builder<String>().build());
    }
  }

  private String parseDiagramId(String id) {
    return id.substring(id.indexOf("-") + 1);
  }

  private String parseNonDiagramId(String id) {
    return id.substring(0, id.indexOf("-"));
  }

}
