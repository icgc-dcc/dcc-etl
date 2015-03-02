/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.indexer.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.test.json.JsonNodes.$;
import static org.icgc.dcc.common.test.json.JsonNodes.convertToString;
import static org.icgc.dcc.etl.indexer.util.JsonNodes.defaultNull;
import lombok.val;

import org.icgc.dcc.common.core.fi.CompositeImpactCategory;
import org.icgc.dcc.common.core.fi.FathmmImpactCategory;
import org.icgc.dcc.common.core.fi.MutationAssessorImpactCategory;
import org.icgc.dcc.common.core.model.ConsequenceType;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonNodesTest {

  @Test
  public void testDefaultNullMissing() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = mapper.createObjectNode();
    String fieldName = "x";

    defaultNull(objectNode, fieldName);

    assertThat(objectNode.has(fieldName));
    assertThat(objectNode.get(fieldName).isNull());
    assertThat(objectNode.toString()).isEqualTo("{\"x\":null}");
  }

  @Test
  public void testDefaultNullPresent() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = mapper.createObjectNode();
    String fieldName = "x";
    objectNode.put(fieldName, 1);

    defaultNull(objectNode, fieldName);

    assertThat(objectNode.has(fieldName));
    assertThat(objectNode.get(fieldName).asInt()).isEqualTo(1);
    assertThat(objectNode.toString()).isEqualTo("{\"x\":1}");
  }

  @Test
  public void testJsonPointer() throws Exception {
    val objectNode = $("{x: [{y: 1}, {y: 2}]}");

    val result = objectNode.at("/x.y");

    System.out.println(result);
  }

  @Test
  public void testPOJONode() throws Exception {
    val objectNode = (ObjectNode) $("{}");
    objectNode.putPOJO("consequence_type", ConsequenceType.FRAMESHIFT_VARIANT);
    objectNode.putPOJO("summary", CompositeImpactCategory.HIGH);
    objectNode.putPOJO("ma", MutationAssessorImpactCategory.HIGH);
    objectNode.putPOJO("fathmm", FathmmImpactCategory.DAMAGING);

    System.out.println(convertToString(objectNode));
  }

}
