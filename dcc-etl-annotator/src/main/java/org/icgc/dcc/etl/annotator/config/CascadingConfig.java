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
package org.icgc.dcc.etl.annotator.config;

import static com.google.common.collect.Maps.newLinkedHashMap;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

import java.util.Map;

import lombok.val;

import org.icgc.dcc.etl.annotator.cascading.TapFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.property.AppProps;

import com.google.common.collect.ImmutableMap;

/**
 * Cascading specific configuration.
 */
@Configuration
public class CascadingConfig {

  /**
   * Dependencies.
   */
  @Value("#{hadoopProperties.properties}")
  private Map<Object, Object> properties;

  @Bean
  public TapFactory tapFactory() {
    return new TapFactory(isLocal());
  }

  @Bean
  public FlowConnector flowConnector() {
    // Adapt to configured platform
    return isLocal() ? new LocalFlowConnector(getProperties()) : new HadoopFlowConnector(getProperties());
  }

  private boolean isLocal() {
    return properties.get(FS_DEFAULT_NAME_KEY).toString().startsWith("file://");
  }

  private Map<Object, Object> getProperties() {
    // Mutable copy for modification
    val copy = newLinkedHashMap(properties);

    // Tell Cascading the jar to use when running within Hadoop
    AppProps.setApplicationJarClass(properties, this.getClass());

    return ImmutableMap.copyOf(copy);
  }

}
