package org.icgc.dcc.etl.core.id;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl.core.id.HttpIdentifierClient.DONOR_ID_PATH;
import static org.icgc.dcc.etl.core.id.HttpIdentifierClient.MUTATION_ID_PATH;
import static org.icgc.dcc.etl.core.id.HttpIdentifierClient.SAMPLE_ID_PATH;
import static org.icgc.dcc.etl.core.id.HttpIdentifierClient.SPECIMEN_ID_PATH;

import org.icgc.dcc.etl.core.id.HttpIdentifierClient.Config;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import lombok.val;

public class HttpIdentifierClientTest {

  private static final int SERVER_PORT = 22223;
  private static final String RESPONSE_ID = "valid response id";

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(SERVER_PORT);

  HttpIdentifierClient client = new HttpIdentifierClient(createClientConfig());

  @Test
  public void testGetDonorId() {
    val requestUrl =
        format("%s?submittedDonorId=%s&submittedProjectId=%s&release=ICGC19&create=false", DONOR_ID_PATH, "s1",
            "p1");
    configureSuccessfulResponse(requestUrl);

    val response = client.getDonorId("s1", "p1");
    assertThat(response.get()).isEqualTo(RESPONSE_ID);
  }

  @Test
  public void testCreateDonorId() {
    val requestUrl =
        format("%s?submittedDonorId=%s&submittedProjectId=%s&release=ICGC19&create=true", DONOR_ID_PATH, "s1",
            "p1");
    configureSuccessfulResponse(requestUrl);

    val response = client.createDonorId("s1", "p1");
    assertThat(response).isEqualTo(RESPONSE_ID);
  }

  @Test
  public void testGetMutationId() {
    val requestUrl = format("%s?chromosome=%s&chromosomeStart=%s&chromosomeEnd=%s&mutation=%s&mutationType=%s&"
        + "assemblyVersion=%s&release=ICGC19&create=false", MUTATION_ID_PATH, "x", "1", "2", "a_b", "ssm", "1");
    configureSuccessfulResponse(requestUrl);

    val response = client.getMutationId("x", "1", "2", "a_b", "ssm", "1");
    assertThat(response.get()).isEqualTo(RESPONSE_ID);
  }

  @Test
  public void testCreateMutationId() {
    val requestUrl = format("%s?chromosome=%s&chromosomeStart=%s&chromosomeEnd=%s&mutation=%s&mutationType=%s&"
        + "assemblyVersion=%s&release=ICGC19&create=true", MUTATION_ID_PATH, "x", "1", "2", "a_b", "ssm", "1");
    configureSuccessfulResponse(requestUrl);

    val response = client.createMutationId("x", "1", "2", "a_b", "ssm", "1");
    assertThat(response).isEqualTo(RESPONSE_ID);
  }

  @Test
  public void testGetSampleId() {
    val requestUrl =
        format("%s?submittedSampleId=%s&submittedProjectId=%s&release=ICGC19&create=false", SAMPLE_ID_PATH, "s1",
            "p1");
    configureSuccessfulResponse(requestUrl);

    val response = client.getSampleId("s1", "p1");
    assertThat(response.get()).isEqualTo(RESPONSE_ID);
  }

  @Test
  public void testCreateSampleId() {
    val requestUrl =
        format("%s?submittedSampleId=%s&submittedProjectId=%s&release=ICGC19&create=true", SAMPLE_ID_PATH, "s1",
            "p1");
    configureSuccessfulResponse(requestUrl);

    val response = client.createSampleId("s1", "p1");
    assertThat(response).isEqualTo(RESPONSE_ID);
  }

  @Test
  public void testGetSpecimenId() {
    val requestUrl =
        format("%s?submittedSpecimenId=%s&submittedProjectId=%s&release=ICGC19&create=false", SPECIMEN_ID_PATH, "s1",
            "p1");
    configureSuccessfulResponse(requestUrl);

    val response = client.getSpecimenId("s1", "p1");
    assertThat(response.get()).isEqualTo(RESPONSE_ID);
  }

  @Test
  public void testCreateSpecimenId() {
    val requestUrl =
        format("%s?submittedSpecimenId=%s&submittedProjectId=%s&release=ICGC19&create=true", SPECIMEN_ID_PATH, "s1",
            "p1");
    configureSuccessfulResponse(requestUrl);

    val response = client.createSpecimenId("s1", "p1");
    assertThat(response).isEqualTo(RESPONSE_ID);
  }

  private void configureSuccessfulResponse(String requestUrl) {
    stubFor(get(urlEqualTo(requestUrl))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "text/plain")
            .withBody(RESPONSE_ID)));
  }

  @Test
  public void test_404() {
    val response = client.getDonorId("123", "ALL-US");
    assertThat(response.isPresent()).isFalse();
  }

  @Test
  public void test_503() {
    val requestUrl =
        format("%s?submittedDonorId=%s&submittedProjectId=%s&release=ICGC19&create=false", DONOR_ID_PATH, "s2", "p2");
    stubFor(get(urlEqualTo(requestUrl))
        .willReturn(aResponse()
            .withStatus(503)));

    client.getDonorId("s2", "p2");
    verify(4, getRequestedFor(urlEqualTo(requestUrl)));
  }

  private static Config createClientConfig() {
    return Config.builder()
        .serviceUrl("http://localhost:" + SERVER_PORT)
        .release("ICGC19")
        .requestLoggingEnabled(true)
        .maxRetries(3)
        .retryMultiplier(1f)
        .waitBeforeRetrySeconds(1)
        .build();
  }

}
