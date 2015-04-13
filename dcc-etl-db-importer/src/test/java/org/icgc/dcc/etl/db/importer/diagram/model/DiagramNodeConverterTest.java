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
package org.icgc.dcc.etl.db.importer.diagram.model;

import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;

import org.junit.Test;

import com.google.common.collect.Maps;

public class DiagramNodeConverterTest {

  @Test
  public void testConvertDiagram() {
    val converter = new DiagramNodeConverter();
    val result = converter.convertDiagram(getTestDiagram("am i aly?", "123,456,789", true), "REACT");

    assertThat(result.toString()).isEqualTo("{\"diagram_id\":\"REACT\",\"xml\":\"am i aly?\",\"highlights\""
        + ":\"123,456,789\",\"protein_map\":{\"123\":\"uniprot1\",\"456\":\"uniprot2\"}}");
  }

  @Test
  public void testConvertIncompleteDiagram() {
    val converter = new DiagramNodeConverter();
    val result = converter.convertDiagram(getTestDiagram("am i aly?", "", false), "REACT");

    assertThat(result.toString()).isEqualTo(
        "{\"diagram_id\":\"REACT\",\"xml\":\"am i aly?\",\"highlights\":\"\"}");
  }

  private Diagram getTestDiagram(String xml, String highlights, Boolean proteins) {
    val diagram = new Diagram();
    diagram.setDiagram(xml);
    diagram.setHighlights(highlights);

    val map = Maps.<String, String> newHashMap();
    if (proteins) {
      map.put("123", "uniprot1");
      map.put("456", "uniprot2");
    }
    diagram.setProteinMap(map);

    return diagram;
  }

}
