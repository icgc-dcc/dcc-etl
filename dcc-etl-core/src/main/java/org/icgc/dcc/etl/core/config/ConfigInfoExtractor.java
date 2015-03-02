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
package org.icgc.dcc.etl.core.config;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.Functions2.castIntegerToString;
import static org.icgc.dcc.common.core.util.URIs.getHost;
import static org.icgc.dcc.common.core.util.URIs.getPassword;
import static org.icgc.dcc.common.core.util.URIs.getPort;
import static org.icgc.dcc.common.core.util.URIs.getUsername;
import static org.icgc.dcc.common.hadoop.util.HadoopConstants.MR_JOBTRACKER_ADDRESS_KEY;

import java.io.File;
import java.net.URI;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.icgc.dcc.common.core.util.Optionals;

import com.google.common.base.Optional;

@NoArgsConstructor(access = PRIVATE)
final class ConfigInfoExtractor {

  enum Type {
    NAMENODE,
    JOBTRACKER,
    ELASTIC_SEARCH,
    MONGO_ETL_NORMAL,
    MONGO_ETL_ADMIN;

    static boolean isValid(String name) {
      try {
        Type.valueOf(name);
      } catch (IllegalArgumentException e) {
        return false;
      }
      return true;
    }
  }

  enum SubType {
    HOST,
    PORT,
    USERNAME,
    PASSWORD;

    // TODO: refactor with same code above
    static boolean isValid(String name) {
      try {
        SubType.valueOf(name);
      } catch (IllegalArgumentException e) {
        return false;
      }
      return true;
    }
  }

  static final Optional<String> getInfo2(
      @NonNull final File configFile,
      @NonNull final Type type,
      @NonNull final SubType subType) {

    val etlConfig = EtlConfigFile.read(configFile);
    val uri = getETLURI(etlConfig, type);

    switch (subType) {
    case HOST:
      return getHost(uri);
    case PORT:
      return Optionals.<Integer, String> cast(
          getPort(uri),
          castIntegerToString());
    case USERNAME:
      return getUsername(uri);
    case PASSWORD:
      return getPassword(uri);
    default:
      throw new UnsupportedOperationException(subType.name());
    }
  }

  private static URI getETLURI(EtlConfig etlConfig, Type type) {
    switch (type) {
    case MONGO_ETL_NORMAL:
      return getQualifiedUri(etlConfig.getReleaseMongoUri());
    case MONGO_ETL_ADMIN:
      return getQualifiedUri(etlConfig.getEtlAdminMongoUri());
    case NAMENODE:
      return getQualifiedUri(etlConfig.getFsUrl());
    case JOBTRACKER:
      return getQualifiedUri(etlConfig.getLoaderHadoop().get(MR_JOBTRACKER_ADDRESS_KEY));
    case ELASTIC_SEARCH:
      return getQualifiedUri(etlConfig.getEsUri());
    default:
      throw new UnsupportedOperationException(type.name());
    }
  }

  @SneakyThrows
  private static URI getQualifiedUri(String fsUri) {
    val schemeSeparator = "://";
    val defaultScheme = "http";
    val defaultPrefix = defaultScheme + schemeSeparator;

    return new URI(fsUri.contains(schemeSeparator) ? fsUri : defaultPrefix + fsUri);
  }

}
