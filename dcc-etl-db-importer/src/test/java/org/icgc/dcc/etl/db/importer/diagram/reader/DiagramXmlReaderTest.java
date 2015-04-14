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

import static org.apache.commons.lang3.StringEscapeUtils.unescapeJson;
import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;

import org.junit.Test;

public class DiagramXmlReaderTest {

  @Test
  public void testXmlReader() throws Exception {
    val reader = new DiagramXmlReader();
    val result = reader.readPathwayXml("4839726");

    assertThat(result).isNotNull();

    // Make sure we can unescape it
    assertThat(unescapeJson(result).trim())
        .isEqualTo(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Process nextId=\"6326\" reactomeDiagramId=\"5084132\" hideCompartmentInName=\"true\">\n  <Properties>\n    <displayName>Diagram of Chromatin organization</displayName>\n  </Properties>\n  <Nodes>\n    <org.gk.render.ProcessNode id=\"354\" reactomeId=\"3247509\" schemaClass=\"Pathway\" position=\"386 120\" bounds=\"310 84 153 73\" textPosition=\"317 99\">\n      <Properties>\n        <displayName>Chromatin modifying enzymes</displayName>\n      </Properties>\n    </org.gk.render.ProcessNode>\n  </Nodes>\n  <Edges />\n  <Pathways />\n</Process>");
  }

}
