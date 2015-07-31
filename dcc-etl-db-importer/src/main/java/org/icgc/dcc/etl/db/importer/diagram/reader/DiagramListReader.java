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

import static org.icgc.dcc.etl.db.importer.diagram.reader.DiagramReader.REACTOME_BASE_URL;

import java.io.IOException;
import java.net.URL;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.db.importer.diagram.model.Pathways;
import org.w3c.dom.Node;

import com.google.common.collect.ImmutableSet;

public class DiagramListReader {

  private final static String DIAGRAMS_LIST_URL = REACTOME_BASE_URL + "pathwayHierarchy/homo+sapiens";
  private final static String DIAGRAMS_ID_CONVERT_URL = REACTOME_BASE_URL + "queryById/Pathway/%s";

  public Pathways readPathwayList() throws IOException, TransformerException {
    val diagrammedPathwaysBuilder = new ImmutableSet.Builder<String>();
    val nonDiagrammedPathwaysBuilder = new ImmutableSet.Builder<String>();

    try {
      val factory = DocumentBuilderFactory.newInstance();
      val doc = factory.newDocumentBuilder().parse(new URL(DIAGRAMS_LIST_URL).openStream());
      val nodes = doc.getElementsByTagName("Pathway");

      // Add all pathway nodes as non-diagrammed or diagrammed
      for (int i = 0; i < nodes.getLength(); i++) {
        val attributes = nodes.item(i).getAttributes();

        val hasDiagram = attributes.getNamedItem("hasDiagram");
        val diagramDbId = attributes.getNamedItem("dbId").getNodeValue();

        if (hasDiagram == null || hasDiagram.getNodeValue() == "false") {
          nonDiagrammedPathwaysBuilder.add(diagramDbId + "-" + getDiagrammedParent(nodes.item(i)));
        } else {
          diagrammedPathwaysBuilder.add(diagramDbId);
        }
      }

      return new Pathways(diagrammedPathwaysBuilder.build(), nonDiagrammedPathwaysBuilder.build());
    } catch (Exception e) {
      throw new RuntimeException("Failed to read list of pathway diagrams", e);
    }
  }

  private String getDiagrammedParent(Node node) {
    val parent = node.getParentNode();
    if (parent == null) {
      // Just in case it has no diagrammed parents... fall back on something
      return "missing";
    }
    val attributes = parent.getAttributes();

    val diagrammed = attributes.getNamedItem("hasDiagram");
    val id = attributes.getNamedItem("dbId").getNodeValue();
    return diagrammed == null ? getDiagrammedParent(parent) : id;
  }

  public String getReactId(@NonNull String dbId) throws IOException {
    return "R-HSA-" + dbId;

    // val querlUrl = new URL(format(DIAGRAMS_ID_CONVERT_URL, dbId));
    // val id = DEFAULT.readTree(querlUrl).path("stableIdentifier").path("displayName").asText();

    // val versionlessId = id.indexOf(".");
    // return id.substring(0, versionlessId);
  }

}
