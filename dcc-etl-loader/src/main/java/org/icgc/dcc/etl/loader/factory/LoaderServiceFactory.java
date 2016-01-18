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
package org.icgc.dcc.etl.loader.factory;

import static com.google.common.base.Strings.isNullOrEmpty;
import static lombok.AccessLevel.PRIVATE;

import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.val;

import org.icgc.dcc.common.hadoop.util.HadoopCompression;
import org.icgc.dcc.etl.loader.config.ConfigModule;
import org.icgc.dcc.etl.loader.config.LoaderModule;
import org.icgc.dcc.etl.loader.service.LoaderService;
import org.icgc.dcc.id.client.http.HttpIdClient;

import com.google.inject.Guice;

@NoArgsConstructor(access = PRIVATE)
public final class LoaderServiceFactory {

  public static final String IDENTIFIER_CLIENT_CLASSNAME_PROPERTY = "dcc.identifier.client.class.name";

  /**
   * Production identifier client.
   */
  private static final String DEFAULT_IDENTIFIER_CLIENT_CLASS_NAME = HttpIdClient.class.getName();

  public static LoaderService createService(
      String fsUrl,
      String fileSystemOutputCompression,
      String releaseMongoUri,
      Map<String, String> hadoop,
      String identifierClientClassName,
      String identifierServiceUri,
      String identifierAuthToken,
      boolean filterAllControlled,
      int maxConcurrentFlows) {

    return Guice.createInjector(
        new ConfigModule(
            fsUrl,
            HadoopCompression.valueOf(fileSystemOutputCompression),
            releaseMongoUri,
            hadoop,
            identifierClientClassName,
            identifierServiceUri,
            identifierAuthToken,
            filterAllControlled,
            maxConcurrentFlows),
        new LoaderModule())
        .getInstance(LoaderService.class);
  }

  public static LoaderService createService(
      String fsUrl,
      String fileSystemOutputCompression,
      String releaseMongoUri,
      Map<String, String> hadoop,
      String identifierServiceUri,
      String identifierAuthToken,
      boolean filterAllControlled,
      int maxConcurrentFlows) {

    return createService(
        fsUrl, fileSystemOutputCompression, releaseMongoUri, hadoop,
        resolveClassName(), identifierServiceUri, identifierAuthToken,
        filterAllControlled, maxConcurrentFlows);
  }

  private static String resolveClassName() {
    val className = System.getProperty(IDENTIFIER_CLIENT_CLASSNAME_PROPERTY);

    return isNullOrEmpty(className) ? DEFAULT_IDENTIFIER_CLIENT_CLASS_NAME : className;
  }

}
