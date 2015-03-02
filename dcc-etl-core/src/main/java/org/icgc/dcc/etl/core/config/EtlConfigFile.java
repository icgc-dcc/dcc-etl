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
package org.icgc.dcc.etl.core.config;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;

import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Splitter;

@Value
public class EtlConfigFile {

  /**
   * Constants.
   */
  public static final String PROPERTY_PREFIX = "dcc.";
  private static final Splitter PROPERTY_SPLITTER = Splitter.on('.').trimResults();
  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

  /**
   * Configuration.
   */
  private final File file;

  @SneakyThrows
  public EtlConfig read() {
    val root = MAPPER.readTree(file);

    for (val entry : System.getProperties().entrySet()) {
      val property = (String) entry.getKey();
      if (property.startsWith(PROPERTY_PREFIX)) {
        val name = property.substring(PROPERTY_PREFIX.length());
        val value = System.getProperty(property);

        addOverride(root, name, value);
      }
    }

    return MAPPER.readValue(new TreeTraversingParser(root), EtlConfig.class);
  }

  @SneakyThrows
  public void write(EtlConfig config) {
    MAPPER.writeValue(file, config);
  }

  private void addOverride(JsonNode root, String name, String value) {
    JsonNode node = root;
    val keys = PROPERTY_SPLITTER.split(name).iterator();

    while (keys.hasNext()) {
      checkArgument(node.isObject(), "Unable to override '%s'; it's not a valid path.", name);
      val obj = (ObjectNode) node;

      val key = keys.next();
      if (keys.hasNext()) {
        JsonNode child = obj.get(key);
        if (child == null) {
          child = obj.objectNode();
          obj.put(key, child);
        }
        node = child;
      } else {
        obj.put(key, value);
      }
    }
  }

  @SneakyThrows
  public static EtlConfig read(File file) {
    return new EtlConfigFile(file).read();
  }

}
