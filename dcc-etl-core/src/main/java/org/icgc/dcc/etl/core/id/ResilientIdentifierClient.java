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
package org.icgc.dcc.etl.core.id;

import java.util.Optional;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.client.apache.ApacheHttpClient;
import com.sun.jersey.client.apache.ApacheHttpClientHandler;
import com.sun.jersey.client.apache.config.DefaultApacheHttpClientConfig;

@Slf4j
public class ResilientIdentifierClient {

  public final static String DONOR_ID_PATH = "/api/donor/id";
  public final static String MUTATION_ID_PATH = "/api/mutation/id";
  public final static String SAMPLE_ID_PATH = "/api/sample/id";
  public final static String SPECIMEN_ID_PATH = "/api/specimen/id";

  private final WebResource resource;
  private final IdentifierClientConfig clientConfig;

  public ResilientIdentifierClient(@NonNull IdentifierClientConfig clientConfig) {
    val client = createAndConfigureClient(clientConfig);
    this.resource = client.resource(clientConfig.getServiceUrl());
    this.clientConfig = clientConfig;
  }

  @NonNull
  public Optional<String> getDonorId(String submittedDonorId, String submittedProjectId) {
    return getDonorId(submittedDonorId, submittedProjectId, false);
  }

  @NonNull
  public String createDonorId(String submittedDonorId, String submittedProjectId) {
    return getDonorId(submittedDonorId, submittedProjectId, true).get();
  }

  private Optional<String> getDonorId(String submittedDonorId, String submittedProjectId, boolean create) {
    val request = resource
        .path(DONOR_ID_PATH)
        .queryParam("submittedDonorId", submittedDonorId)
        .queryParam("submittedProjectId", submittedProjectId)
        .queryParam("create", String.valueOf(create));

    return getResponse(request, RetryContext.create(clientConfig));
  }

  @NonNull
  public Optional<String> getSampleId(String submittedSampleId, String submittedProjectId) {
    return getSampleId(submittedSampleId, submittedProjectId, false);
  }

  @NonNull
  public String createSampleId(String submittedSampleId, String submittedProjectId) {
    return getSampleId(submittedSampleId, submittedProjectId, true).get();
  }

  private Optional<String> getSampleId(String submittedSampleId, String submittedProjectId, boolean create) {
    val request = resource
        .path(SAMPLE_ID_PATH)
        .queryParam("submittedSampleId", submittedSampleId)
        .queryParam("submittedProjectId", submittedProjectId)
        .queryParam("create", String.valueOf(create));

    return getResponse(request, RetryContext.create(clientConfig));
  }

  @NonNull
  public Optional<String> getSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    return getSpecimenId(submittedSpecimenId, submittedProjectId, false);
  }

  @NonNull
  public String createSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    return getSpecimenId(submittedSpecimenId, submittedProjectId, true).get();
  }

  private Optional<String> getSpecimenId(String submittedSpecimenId, String submittedProjectId, boolean create) {
    val request = resource
        .path(SPECIMEN_ID_PATH)
        .queryParam("submittedSpecimenId", submittedSpecimenId)
        .queryParam("submittedProjectId", submittedProjectId)
        .queryParam("create", String.valueOf(create));

    return getResponse(request, RetryContext.create(clientConfig));
  }

  @NonNull
  public Optional<String> getMutationId(String chromosome, String chromosomeStart, String chromosomeEnd,
      String mutation, String mutationType, String assemblyVersion) {
    return getMutationId(chromosome, chromosomeStart, chromosomeEnd, mutation, mutationType, assemblyVersion, false);
  }

  @NonNull
  public String createMutationId(String chromosome, String chromosomeStart, String chromosomeEnd, String mutation,
      String mutationType, String assemblyVersion) {
    return getMutationId(chromosome, chromosomeStart, chromosomeEnd, mutation, mutationType, assemblyVersion, true)
        .get();
  }

  private Optional<String> getMutationId(String chromosome, String chromosomeStart, String chromosomeEnd,
      String mutation, String mutationType, String assemblyVersion, boolean create) {
    val request = resource
        .path(MUTATION_ID_PATH)
        .queryParam("chromosome", chromosome)
        .queryParam("chromosomeStart", chromosomeStart)
        .queryParam("chromosomeEnd", chromosomeEnd)
        .queryParam("mutation", mutation)
        .queryParam("mutationType", mutationType)
        .queryParam("assemblyVersion", assemblyVersion)
        .queryParam("create", String.valueOf(create));

    return getResponse(request, RetryContext.create(clientConfig));
  }

  private Optional<String> getResponse(WebResource request, RetryContext retryContext) {
    val response = request.get(ClientResponse.class);
    if (response.getStatus() == 404) {
      return Optional.empty();
    }

    if (response.getStatus() == 503 && retryContext.isRetry()) {
      return getResponse(request, waitBeforeRetry(retryContext));
    }

    val entity = response.getEntity(String.class);

    return Optional.of(entity);
  }

  @SneakyThrows
  private RetryContext waitBeforeRetry(RetryContext retryContext) {
    log.info("Service Unavailable. Waiting for {} seconds before retry...", retryContext.getSleepSeconds());
    log.info("{}", retryContext);
    Thread.sleep(retryContext.sleepSeconds * 1000);

    return RetryContext.next(retryContext);
  }

  private static Client createAndConfigureClient(IdentifierClientConfig config) {
    val connectionManager = new MultiThreadedHttpConnectionManager();
    connectionManager.getParams().setConnectionTimeout(5000);
    connectionManager.getParams().setSoTimeout(1000);
    connectionManager.getParams().setDefaultMaxConnectionsPerHost(10);

    val httpClient = new HttpClient(connectionManager);
    val clientHandler = new ApacheHttpClientHandler(httpClient);
    val root = new ApacheHttpClient(clientHandler);
    val clientConfig = new DefaultApacheHttpClientConfig();
    val client = new Client(root, clientConfig);

    if (config.isRequestLoggingEnabled()) {
      client.addFilter(new LoggingFilter());
    }

    return client;
  }

  @Value
  @Builder
  private final static class RetryContext {

    int attempts;
    int sleepSeconds;
    float multiplier;
    boolean retry;

    public static RetryContext create(IdentifierClientConfig clientConfig) {
      return RetryContext.builder()
          .attempts(clientConfig.getMaxRetries())
          .sleepSeconds(clientConfig.getWaitBeforeRetrySeconds())
          .multiplier(clientConfig.getRetryMultiplier())
          .retry(clientConfig.getMaxRetries() > 0)
          .build();
    }

    public static RetryContext next(RetryContext previousContext) {
      return RetryContext.builder()
          .retry(previousContext.attempts > 1)
          .attempts(previousContext.attempts - 1)
          .sleepSeconds((int) (previousContext.sleepSeconds * previousContext.multiplier))
          .multiplier(previousContext.multiplier)
          .build();
    }

  }

}
