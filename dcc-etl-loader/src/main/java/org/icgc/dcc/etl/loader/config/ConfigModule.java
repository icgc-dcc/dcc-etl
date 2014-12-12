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
package org.icgc.dcc.etl.loader.config;

import static com.google.inject.name.Names.named;
import static org.icgc.dcc.common.core.model.Configurations.FILTER_ALL_CONTROLLED;
import static org.icgc.dcc.common.core.model.Configurations.HADOOP_KEY;
import static org.icgc.dcc.common.core.model.Configurations.IDENTIFIER_CLIENT_CLASS_NAME_KEY;
import static org.icgc.dcc.common.core.model.Configurations.IDENTIFIER_KEY;
import static org.icgc.dcc.common.core.model.Configurations.LOADER_MAX_CONCURRENT_FLOWS;
import static org.icgc.dcc.common.core.model.Configurations.RELEASE_MONGO_URI_KEY;

import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.icgc.dcc.common.hadoop.util.HadoopCompression;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.typesafe.config.Config;

/**
 * Makes {@code Config} injectable instead of accessible as a singleton.
 */
@RequiredArgsConstructor
@Getter
public class ConfigModule extends AbstractModule {

  private static final TypeLiteral<Map<String, String>> STRING_MAP =
      new TypeLiteral<Map<String, String>>() {};

  private final String fsUrl;
  private final HadoopCompression fileSystemOutputCompression;
  private final String releaseMongoUri;
  private final Map<String, String> hadoopProperties;
  private final String identifierClientClassName;
  private final String identifierServiceUri;
  private final Boolean filterAllControlled;
  private final int maxConcurrentFlows;

  @Override
  protected void configure() {
    bind(Config.class)
        .toInstance(new LegacyConfig(this));
    bind(String.class)
        .annotatedWith(named(RELEASE_MONGO_URI_KEY))
        .toInstance(releaseMongoUri);

    // Bind hadoop related instances
    bind(HadoopCompression.class).toInstance(fileSystemOutputCompression);
    bind(STRING_MAP)
        .annotatedWith(named(HADOOP_KEY))
        .toInstance(hadoopProperties);

    bind(String.class)
        .annotatedWith(named(IDENTIFIER_CLIENT_CLASS_NAME_KEY))
        .toInstance(identifierClientClassName);
    bind(String.class)
        .annotatedWith(named(IDENTIFIER_KEY))
        .toInstance(identifierServiceUri);

    bind(Boolean.class)
        .annotatedWith(named(FILTER_ALL_CONTROLLED))
        .toInstance(filterAllControlled);
    bind(Integer.class)
        .annotatedWith(named(LOADER_MAX_CONCURRENT_FLOWS))
        .toInstance(maxConcurrentFlows);
  }

}
