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

import static com.google.common.collect.ImmutableSet.copyOf;
import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.Jackson.DEFAULT;
import static org.icgc.dcc.etl.db.importer.diagram.reader.DiagramReader.REACTOME_BASE_URL;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import lombok.NonNull;
import lombok.val;

import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class DiagramListReader {

  private final String DIAGRAMS_LIST_URL = REACTOME_BASE_URL + "pathwayHierarchy/homo+sapiens";
  private final String DIAGRAMS_ID_CONVERT_URL = REACTOME_BASE_URL + "queryById/Pathway/%s";
  private List<String> diagrammedPathways;
  private List<String> nonDiagrammedPathways;

  public void readPathwayList() throws IOException, TransformerException {
    diagrammedPathways = new ArrayList<String>();
    nonDiagrammedPathways = new ArrayList<String>();

    try {
      val factory = DocumentBuilderFactory.newInstance();
      val doc = factory.newDocumentBuilder().parse(new URL(DIAGRAMS_LIST_URL).openStream());
      val nodes = doc.getElementsByTagName("Pathway");

      // Add all pathway nodes as non-diagrammed or diagrammed
      for (int i = 0; i < nodes.getLength(); i++) {
        val hasDiagram = nodes.item(i).getAttributes().getNamedItem("hasDiagram");

        if (hasDiagram == null) {
          nonDiagrammedPathways.add(nodes.item(i).getAttributes().getNamedItem("dbId").getNodeValue()
              + "-" + getDiagrammedParent(nodes.item(i))); // Append the diagrammed parent
        } else {
          diagrammedPathways.add(nodes.item(i).getAttributes().getNamedItem("dbId").getNodeValue());
        }
      }

      makeUnique();

    } catch (SAXException | IOException | ParserConfigurationException e) {
      throw new IOException("Failed to read list of pathway diagrams", e);
    }
  }

  private void makeUnique() {
    // Make the diagrammed list unique (removes about 15%)
    val diagrammedSet = copyOf(diagrammedPathways);
    diagrammedPathways.clear();
    diagrammedPathways.addAll(diagrammedSet);

    // Make non diagrammed list unique too (removes about 25%)
    val nonDiagrammedSet = copyOf(nonDiagrammedPathways);
    nonDiagrammedPathways.clear();
    nonDiagrammedPathways.addAll(nonDiagrammedSet);
  }

  private String getDiagrammedParent(Node node) {
    val parent = node.getParentNode();
    if (parent == null) {
      // Just in case it has no diagrammed parents... fall back on something
      return "missing";
    }
    val attribute = parent.getAttributes().getNamedItem("hasDiagram");
    return attribute == null ? getDiagrammedParent(parent) : attribute.getNodeValue();
  }

  public List<String> getDiagrammedPathways() {
    return diagrammedPathways;
  }

  public List<String> getNonDiagrammedPathways() {
    return nonDiagrammedPathways;
  }

  public String getReactId(@NonNull String dbId) throws IOException {
    val querlUrl = new URL(format(DIAGRAMS_ID_CONVERT_URL, dbId));
    val id = DEFAULT.readTree(querlUrl).path("stableIdentifier").path("displayName").asText();

    return id.substring(0, id.indexOf("."));
  }

}
