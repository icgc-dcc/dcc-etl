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
package org.icgc.dcc.etl.core.util;

import static com.google.common.collect.Lists.newArrayList;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * ICGCClientConfigs class for mongodb related operations.
 */
public class MongoDbUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static String fields(String includedField) {
    return fields(newArrayList(includedField));
  }

  public static String fields(List<String> includedFields) {
    return fields(new ArrayList<String>(), includedFields);
  }

  /**
   * Returns a {@link String} representing the selection/un-selection of fields using mongodb's syntax.
   * <p>
   * For instance: {"field1": 1, "field3": 0, "field4": 0, "field5": 1} where fields 1 and 5 are explicitly selected and
   * fields 3 and 4 explicitly unselected.
   */
  public static String fields(List<String> excludedFields, List<String> includedFields) {
    ObjectNode fields = MAPPER.createObjectNode();

    for (String excludedField : excludedFields) {
      fields.put(excludedField, 0);
    }
    for (String excludedField : includedFields) {
      fields.put(excludedField, 1);
    }

    return fields.toString();
  }

}
