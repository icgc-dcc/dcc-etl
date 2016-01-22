/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.icgc.dcc.etl.indexer.schema;

import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SchemaBufferTest {

  static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testGetSchemaWithPOJONodes() {
    val buffer = new SchemaBuffer();

    // Setup
    val document = MAPPER.createObjectNode();
    document.putPOJO("list", ImmutableList.of(1, 2, 3));
    document.putPOJO("map", ImmutableMap.of("x", 1, "y", 3));
    document.putPOJO("number", 1);
    document.putPOJO("string", "x");

    // Exercise
    buffer.addDocument(document);

    // Verify
    val schema = buffer.getSchema().iterator();
    assertThat(schema.next()).isEqualTo("list.$: NUMBER");
    assertThat(schema.next()).isEqualTo("list: ARRAY");
    assertThat(schema.next()).isEqualTo("map.x: NUMBER");
    assertThat(schema.next()).isEqualTo("map.y: NUMBER");
    assertThat(schema.next()).isEqualTo("number: NUMBER");
    assertThat(schema.next()).isEqualTo("string: STRING");
  }

}
