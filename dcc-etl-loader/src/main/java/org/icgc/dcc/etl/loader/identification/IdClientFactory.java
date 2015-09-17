/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.loader.identification;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;

import java.io.Serializable;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.icgc.dcc.id.client.core.IdClient;
import org.icgc.dcc.id.client.http.HttpIdClient;
import org.icgc.dcc.id.client.util.HashIdClient;

@RequiredArgsConstructor
public class IdClientFactory implements Serializable {

  private static final String HASH_ID_CLIENT_CLASSNAME = HashIdClient.class.getName();
  private static final String HTTP_ID_CLIENT_CLASSNAME = HttpIdClient.class.getName();

  @NonNull
  private final String className;
  @NonNull
  private final String serviceUri;
  @NonNull
  private final String release;
  private final String authToken;

  public IdClient create() {
    if (className.equals(HTTP_ID_CLIENT_CLASSNAME)) {
      return createHttpIdClient();
    } else if (className.equals(HASH_ID_CLIENT_CLASSNAME)) {
      return createHashIdClient();
    } else {
      throw new IllegalArgumentException(format("%s client is not supported", className));
    }
  }

  private IdClient createHashIdClient() {
    return new HashIdClient(serviceUri, release);
  }

  private IdClient createHttpIdClient() {
    checkArgument(!isNullOrEmpty(authToken), "Identifier Auth Token is not defined");
    return new HttpIdClient(serviceUri, release, authToken);
  }

}
