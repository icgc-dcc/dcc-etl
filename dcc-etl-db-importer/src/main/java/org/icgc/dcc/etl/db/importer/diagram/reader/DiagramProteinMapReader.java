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

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import lombok.val;

import org.elasticsearch.common.collect.Maps;
import org.xml.sax.SAXException;

/**
 * 
 */
public class DiagramProteinMapReader {

  private final String PROTEIN_MAP_URL =
      "http://reactomews.oicr.on.ca:8080/ReactomeRESTfulAPI/RESTfulWS/getPhysicalToReferenceEntityMaps/%s";
  private Map<String, String> proteinMap = Maps.newHashMap();

  public void readProteinMap(String pathwayId) throws IOException {
    try {
      val factory = DocumentBuilderFactory.newInstance();
      val doc = factory.newDocumentBuilder().parse(new URL(format(PROTEIN_MAP_URL, pathwayId)).openStream());
      val nodes = doc.getElementsByTagName("refEntities");

      for (int i = 0; i < nodes.getLength(); i++) {
        String reference = "uniprot", db = "dbid";
        Boolean isProtein = false;
        val children = nodes.item(i).getChildNodes();

        for (int j = 0; j < children.getLength(); j++) {
          if (children.item(j).getNodeName().equalsIgnoreCase("dbId")) {
            db = children.item(j).getNodeValue();
          } else if (children.item(j).getNodeName().equalsIgnoreCase("displayName")) {
            reference = children.item(j).getNodeValue().substring(0, children.item(j).getNodeValue().indexOf(" "));
          } else if (children.item(j).getNodeName().equalsIgnoreCase("schemaClass")) {
            isProtein = children.item(j).getNodeValue().equalsIgnoreCase("ReferenceGeneProduct");
          }
        }

        // Only add it to the map if it's not there
        if (isProtein && proteinMap.get(reference) == null) {
          proteinMap.put(reference, db);
        }
      }

    } catch (SAXException | IOException | ParserConfigurationException e) {
      throw new IOException(format("Failed to get protein map with id %s", pathwayId), e);
    }
  }

  public Map<String, String> getProteinMap() {
    return proteinMap;
  }

}
