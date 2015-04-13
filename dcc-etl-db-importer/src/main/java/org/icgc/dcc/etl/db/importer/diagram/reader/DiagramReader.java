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

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.diagram.model.Diagram;
import org.icgc.dcc.etl.db.importer.diagram.model.DiagramModel;

@Slf4j
public class DiagramReader {

  public final static String REACTOME_BASE_URL = "http://reactomews.oicr.on.ca:8080/ReactomeRESTfulAPI/RESTfulWS/";

  public DiagramModel read() throws Exception {
    val model = new DiagramModel();
    val listReader = new DiagramListReader();

    log.info("Reading list of pathways..");
    listReader.readPathwayList();

    log.info("Getting all diagrammed pathways and their protein maps...");
    for (val pathwayId : listReader.getDiagrammedPathways()) {
      val diagram = new Diagram();

      diagram.setDiagram(new DiagramXmlReader().readPathwayXml(pathwayId));
      diagram.setProteinMap(new DiagramProteinMapReader().readProteinMap(pathwayId));
      diagram.setHighlights("");

      model.addDiagram(pathwayId, diagram);

      Thread.sleep(3000); // To avoid crashing reactome's servers
    }

    log.info("Adding all non-diagrammed pathways and their highlights...");
    for (val id : listReader.getNonDiagrammedPathways()) {
      val diagrammedId = id.substring(id.indexOf("-") + 1);
      val nonDiagrammedId = id.substring(0, id.indexOf("-"));

      val baseDiagram = model.getDiagrams().get(diagrammedId);
      baseDiagram.setHighlights(new DiagramHighlightReader().readHighlights(nonDiagrammedId));

      model.addDiagram(nonDiagrammedId, baseDiagram);
    }

    val updatedModel = new DiagramModel();

    log.info("Replacing all dbIds with REACT ids...");
    for (val entry : model.getDiagrams().entrySet()) {
      val reactId = listReader.getReactId(entry.getKey());
      updatedModel.addDiagram(reactId, entry.getValue());
    }

    return updatedModel;
  }

}
