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

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.client.apache.ApacheHttpClient;
import com.sun.jersey.client.apache.ApacheHttpClientHandler;
import com.sun.jersey.client.apache.config.DefaultApacheHttpClientConfig;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpIdentifierClient implements IdentifierClient {

  /**
   * Constants.
   */
  public final static String DONOR_ID_PATH = "/api/donor/id";
  public final static String SPECIMEN_ID_PATH = "/api/specimen/id";
  public final static String SAMPLE_ID_PATH = "/api/sample/id";
  public final static String MUTATION_ID_PATH = "/api/mutation/id";

  /**
   * State.
   */
  private final Client client;
  private final WebResource resource;
  private final String release;
  private final Config clientConfig;

  public HttpIdentifierClient(@NonNull Config clientConfig) {
    this.client = createClient(clientConfig);
    this.release = clientConfig.getRelease();
    this.resource = client.resource(clientConfig.getServiceUrl());
    this.clientConfig = clientConfig;
  }

  /**
   * Required for reflection in Loader
   */
  public HttpIdentifierClient(@NonNull String serviceUri, String release) {
    this(Config.builder()
        .serviceUrl(serviceUri)
        .release(release)
        .build());
  }

  @Override
  public Optional<String> getDonorId(@NonNull String submittedDonorId, @NonNull String submittedProjectId) {
    return getDonorId(submittedDonorId, submittedProjectId, false);
  }

  @Override
  @NonNull
  public String createDonorId(String submittedDonorId, String submittedProjectId) {
    return getDonorId(submittedDonorId, submittedProjectId, true).get();
  }

  private Optional<String> getDonorId(@NonNull String submittedDonorId, @NonNull String submittedProjectId,
      boolean create) {
    val request = resource
        .path(DONOR_ID_PATH)
        .queryParam("submittedDonorId", submittedDonorId)
        .queryParam("submittedProjectId", submittedProjectId)
        .queryParam("release", release)
        .queryParam("create", String.valueOf(create));

    return getResponse(request, RetryContext.create(clientConfig));
  }

  @Override
  public Optional<String> getSpecimenId(@NonNull String submittedSpecimenId, @NonNull String submittedProjectId) {
    return getSpecimenId(submittedSpecimenId, submittedProjectId, false);
  }

  @Override
  public String createSpecimenId(@NonNull String submittedSpecimenId, @NonNull String submittedProjectId) {
    return getSpecimenId(submittedSpecimenId, submittedProjectId, true).get();
  }

  private Optional<String> getSpecimenId(String submittedSpecimenId, String submittedProjectId, boolean create) {
    val request = resource
        .path(SPECIMEN_ID_PATH)
        .queryParam("submittedSpecimenId", submittedSpecimenId)
        .queryParam("submittedProjectId", submittedProjectId)
        .queryParam("release", release)
        .queryParam("create", String.valueOf(create));

    return getResponse(request, RetryContext.create(clientConfig));
  }

  @Override
  public Optional<String> getSampleId(@NonNull String submittedSampleId, @NonNull String submittedProjectId) {
    return getSampleId(submittedSampleId, submittedProjectId, false);
  }

  @Override
  public String createSampleId(@NonNull String submittedSampleId, @NonNull String submittedProjectId) {
    return getSampleId(submittedSampleId, submittedProjectId, true).get();
  }

  private Optional<String> getSampleId(String submittedSampleId, String submittedProjectId, boolean create) {
    val request = resource
        .path(SAMPLE_ID_PATH)
        .queryParam("submittedSampleId", submittedSampleId)
        .queryParam("submittedProjectId", submittedProjectId)
        .queryParam("release", release)
        .queryParam("create", String.valueOf(create));

    return getResponse(request, RetryContext.create(clientConfig));
  }

  @Override
  public Optional<String> getMutationId(@NonNull String chromosome, @NonNull String chromosomeStart,
      @NonNull String chromosomeEnd,
      @NonNull String mutation, @NonNull String mutationType, @NonNull String assemblyVersion) {
    return getMutationId(chromosome, chromosomeStart, chromosomeEnd, mutation, mutationType, assemblyVersion, false);
  }

  @Override
  public String createMutationId(@NonNull String chromosome, @NonNull String chromosomeStart,
      @NonNull String chromosomeEnd, @NonNull String mutation,
      @NonNull String mutationType, @NonNull String assemblyVersion) {
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
        .queryParam("release", release)
        .queryParam("create", String.valueOf(create));

    return getResponse(request, RetryContext.create(clientConfig));
  }

  private Optional<String> getResponse(WebResource request, RetryContext retryContext) {
    try {
      val response = request.get(ClientResponse.class);
      if (response.getStatus() == 404) {
        return Optional.empty();
      }

      if (response.getStatus() == 503 && retryContext.isRetry()) {
        return getResponse(request, waitBeforeRetry(retryContext));
      }

      val entity = response.getEntity(String.class);

      return Optional.of(entity);
    } catch (Exception e) {
      log.info("Error requesting {}, {}: {}", request, retryContext, e);
      throw new RuntimeException(e);
    }
  }

  @SneakyThrows
  private RetryContext waitBeforeRetry(RetryContext retryContext) {
    log.info("Service Unavailable. Waiting for {} seconds before retry...", retryContext.getSleepSeconds());
    log.info("{}", retryContext);
    Thread.sleep(retryContext.sleepSeconds * 1000);

    return RetryContext.next(retryContext);
  }

  private static Client createClient(Config config) {
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

  @Override
  public void close() throws IOException {
    log.info("Destroying client...");
    client.destroy();
    log.info("Client destroyed");
  }

  @Value
  @Builder
  public static class Config {

    String serviceUrl;
    String release;

    int maxRetries;
    int waitBeforeRetrySeconds;
    float retryMultiplier;
    boolean requestLoggingEnabled;

    public static ConfigBuilder builder() {
      val builder = new ConfigBuilder();
      builder.requestLoggingEnabled(false);
      builder.maxRetries(10);
      builder.waitBeforeRetrySeconds(15);
      builder.retryMultiplier(2.0f);

      return builder;
    }

  }

  @Value
  @Builder
  private final static class RetryContext {

    int attempts;
    int sleepSeconds;
    float multiplier;
    boolean retry;

    public static RetryContext create(Config clientConfig) {
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
