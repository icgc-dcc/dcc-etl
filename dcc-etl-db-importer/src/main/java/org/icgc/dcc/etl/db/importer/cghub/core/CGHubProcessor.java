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
package org.icgc.dcc.etl.db.importer.cghub.core;

import java.io.InputStream;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

@Slf4j
public class CGHubProcessor {

  /**
   * The name of the project parameter (classified by disease) used with the {@link #RESOURCE_NAME}.
   */
  private static final String PROJECT_PARAM_NAME = "disease_abbr";

  /**
   * The base URL of the CGHub API.
   */
  private static final String DEFAULT_URL = "https://cghub.ucsc.edu/cghub/metadata";

  /**
   * The name of the source HTTP resource.
   */
  private static final String RESOURCE_NAME = "analysisObject";

  @NonNull
  private final Client client;
  @NonNull
  private final WebResource resource;
  @NonNull
  private final String url;

  public CGHubProcessor() {
    this(DEFAULT_URL);
  }

  public CGHubProcessor(String url) {
    this(url, Client.create());
  }

  public CGHubProcessor(String url, Client client) {
    this.url = url;
    this.client = client;
    this.resource = client.resource(url).path(RESOURCE_NAME);
  }

  public InputStream getProject(String diseaseCode) {
    val request = createProjectRequest(diseaseCode);

    log.info("Finding project '{}' from '{}'...", diseaseCode, request);
    val response = request.get(ClientResponse.class);

    return response.getEntityInputStream();
  }

  private WebResource createProjectRequest(String diseaseCode) {
    return resource.queryParam(PROJECT_PARAM_NAME, diseaseCode);
  }

}
