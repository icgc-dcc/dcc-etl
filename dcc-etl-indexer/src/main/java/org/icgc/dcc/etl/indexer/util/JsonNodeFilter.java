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

import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.List;

import lombok.Data;
import lombok.NonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.nebhale.jsonpath.JsonPath;

public class JsonNodeFilter implements Serializable {

  @Data
  private static class FilterPath implements Serializable {

    @NonNull
    final JsonPath path;

    @NonNull
    final String field;
  }

  private final List<FilterPath> filterPaths;

  public JsonNodeFilter(String... expressions) {
    Builder<FilterPath> builder = ImmutableList.builder();

    for (String expression : expressions) {
      int i = expression.lastIndexOf(".");
      checkState(i > 0, "Expression '%s' must have at least one sub-path for filtering", expression);

      String path = expression.substring(0, i);
      String field = expression.substring(i + 1);

      FilterPath filterPath = new FilterPath(JsonPath.compile(path), field);
      builder.add(filterPath);
    }

    this.filterPaths = builder.build();
  }

  public void filter(JsonNode target) {
    for (FilterPath filterPath : filterPaths) {
      String field = filterPath.getField();
      JsonPath path = filterPath.getPath();
      JsonNode result = path.read(target, JsonNode.class);

      final boolean missing = result == null;
      if (missing) {
        continue;
      }

      if (result.isObject()) {
        // Single node
        ((ObjectNode) result).remove(field);
      } else if (result.isArray()) {
        // Multiple nodes
        for (JsonNode node : result) {
          ((ObjectNode) node).remove(field);
        }
      }
    }
  }

}
