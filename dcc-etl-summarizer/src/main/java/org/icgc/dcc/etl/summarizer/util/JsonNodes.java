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
package org.icgc.dcc.etl.summarizer.util;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static lombok.AccessLevel.PRIVATE;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;

/**
 * Common utilities for working with {@code JsonNode}s.
 */
@NoArgsConstructor(access = PRIVATE)
public final class JsonNodes {

  public static void increment(ObjectNode object, String fieldName) {
    JsonNode field = object.get(fieldName);
    if (field.isInt()) {
      int value = field.intValue();

      object.put(fieldName, value + 1);
    } else if (field.isLong()) {
      long value = field.longValue();

      object.put(fieldName, value + 1L);
    } else {
      throw new UnsupportedOperationException("Cannot increment field '" + fieldName + "' in object: " + object);
    }
  }

  public static boolean contains(ArrayNode array, JsonNode node) {
    for (JsonNode element : array) {
      if (element.equals(node)) {
        return true;
      }
    }

    return false;
  }

  public static Set<String> extractStudies(JsonNode jsonNode) {
    Set<String> result = Sets.newHashSet();
    for (JsonNode element : jsonNode) {
      if (element.isArray()) {
        result.addAll(extractStudies(element));
      }
      val text = element.textValue();
      if (text != null && !text.equals("")) {
        result.add(text);
      }
    }

    return result;
  }

  public static List<String> textValues(JsonNode jsonNode) {
    List<String> list = newArrayList();
    for (JsonNode element : jsonNode) {
      list.add(element.textValue());
    }

    return unmodifiableList(list);
  }

  public static List<String> textValues(String fieldName, Iterable<JsonNode> nodes) {
    List<String> list = newArrayList();
    for (JsonNode node : nodes) {
      list.add(node.path(fieldName).textValue());
    }

    return unmodifiableList(list);
  }

  public static Map<String, String> mapTextValues(String keyFieldName, String valueFieldName, Iterable<JsonNode> nodes) {
    Map<String, String> map = newHashMap();
    for (val node : nodes) {
      String key = node.path(keyFieldName).asText();
      String value = node.path(valueFieldName).asText();

      map.put(key, value);
    }

    return unmodifiableMap(map);
  }

  public static Map<String, List<String>> mapMultipleTextValues(String keyFieldName, String valueFieldName,
      Iterable<JsonNode> nodes) {
    Map<String, List<String>> map = newHashMap();
    for (val node : nodes) {
      String key = node.path(keyFieldName).asText();
      List<String> value = textValues(node.path(valueFieldName));

      map.put(key, value);
    }

    return unmodifiableMap(map);
  }

  public static Map<String, Long> mapLongValues(String keyFieldName, String valueFieldName, Iterable<JsonNode> nodes) {
    Map<String, Long> map = newHashMap();
    for (val node : nodes) {
      String key = node.path(keyFieldName).asText();
      Long value = node.path(valueFieldName).asLong();

      map.put(key, value);
    }

    return unmodifiableMap(map);
  }

}
