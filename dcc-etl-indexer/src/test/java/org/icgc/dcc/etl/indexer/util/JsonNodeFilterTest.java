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
package org.icgc.dcc.etl.indexer.util;

import static org.icgc.dcc.common.test.fest.JsonNodeAssert.assertThat;
import static org.icgc.dcc.common.test.json.JsonNodes.$;

import org.icgc.dcc.etl.indexer.util.JsonNodeFilter;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonNodeFilterTest {

  @Test
  public void test() {
    // @formatter:off
    
    // Test data
    JsonNode root = $(
      "{" +
        "a: {" +
          "b: 1," +
          "c: [" +
            "{" +
              "d: 2," +
              "e: 3" +
            "}," +
            "{" +
              "d: 4," +
              "e: 5" +
            "}" +
          "]" +
        "} " +
      "}"
    );

    // Apply all JsonPath filters listed to root
    filter(root, 
      "$.a.b", 
      "$.a.c[*].d"
    );

    // Verify
    assertThat(root).isEqualTo($(
      "{" +
        "a: {" +
          "c: [" +
            "{" +
              "e: 3" +
            "}," +
            "{" +
              "e: 5" +
            "}" +
          "]" +
        "} " +
      "}"
    ));
    // @formatter:on
  }

  private void filter(JsonNode root, String... expressions) {
    JsonNodeFilter filter = new JsonNodeFilter(expressions);
    filter.filter(root);
  }

}
