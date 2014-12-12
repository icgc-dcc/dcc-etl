/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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

import static com.google.common.collect.Sets.newTreeSet;

import java.util.Set;
import java.util.Stack;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.google.common.base.Joiner;

/**
 * A document instance derived buffer for inferring a document schema give a training set of document instances.
 */
@RequiredArgsConstructor
public class SchemaBuffer {

  /**
   * Constants.
   */
  private static final String ARRAY_TOKEN = "$";
  private static final String PATH_TYPE_SEPARATOR = ": ";
  private static final Joiner JOINER = Joiner.on('.');
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * State.
   */
  @Getter
  private final Set<String> schema = newTreeSet();

  public void addDocument(ObjectNode document) {
    val path = new Stack<String>();
    processDocument(path, document);
  }

  private void processDocument(Stack<String> path, JsonNode node) {
    if (node.isArray()) {
      schema.add(formatPath(path, node));
      if (node.size() > 0) {
        val element = node.get(0);

        path.push(ARRAY_TOKEN);
        if (element.isObject() || element.isArray()) {
          processDocument(path, element);
        } else {
          schema.add(formatPath(path, element));
        }
        path.pop();
      }
    } else if (node.isObject()) {
      val iterator = node.fieldNames();
      while (iterator.hasNext()) {
        val fieldName = iterator.next();
        val field = node.get(fieldName);

        path.push(fieldName);
        processDocument(path, field);
        path.pop();
      }
    } else if (node.isPojo()) {
      val value = ((POJONode) node).getPojo();
      val convertedNode = MAPPER.convertValue(value, JsonNode.class);

      processDocument(path, convertedNode);
    } else if (!node.isNull()) {
      schema.add(formatPath(path, node));
    }
  }

  private String formatPath(Stack<String> path, JsonNode node) {
    return JOINER.join(path) + PATH_TYPE_SEPARATOR + node.getNodeType();
  }

}
