package org.icgc.dcc.etl.indexer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl.indexer.factory.DocumentServiceFactory.newDistributedService;
import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.GENE_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.MUTATION_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.OBSERVATION_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.RELEASE_TYPE;

import java.io.IOException;
import java.net.URISyntaxException;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.icgc.dcc.etl.indexer.task.DistributedDocumentTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.hadoop.util.HadoopUtil;

@Slf4j
public class DistributedDocumentServiceTest extends AbstractDocumentServiceTest {

  @Before
  public void setup() {
    this.service = newDistributedService(config);
  }

  @After
  public void tearDown() throws IOException {
    service.close();
  }

  @Test
  public void testWriteDocuments() throws IOException {
    test(DocumentType.values());
  }

  @Test
  public void testWriteDonorDocuments() throws IOException {
    test(DONOR_TYPE);
  }

  @Test
  public void testWriteDonorCentricDocuments() throws IOException {
    test(DONOR_CENTRIC_TYPE);
  }

  @Test
  public void testWriteGeneCentricDocuments() throws IOException {
    test(GENE_CENTRIC_TYPE);
  }

  @Test
  public void testWriteObservationCentricDocuments() throws IOException {
    test(OBSERVATION_CENTRIC_TYPE);
  }

  @Test
  public void testWriteMutationCentricDocuments() throws IOException {
    test(MUTATION_CENTRIC_TYPE);
  }

  @Test
  public void testSerializable() throws URISyntaxException {
    val task = new DistributedDocumentTask(RELEASE_TYPE, config);

    val serialized = cascadingSerialize(task);
    log.info("task: {}, serialized: {}", task, serialized);
    assertThat(serialized).isNotEmpty();

    val deserialized = cascadingDeserialize(serialized);
    log.info("task: {}, deserialized: {}", task, deserialized);
    assertThat(deserialized).isEqualTo(task);
  }

  /**
   * Method that reproduces what Cascading does to serialize a job step.
   * 
   * @param object the object to serialize.
   * @return the base 64 encoded object
   */
  @SneakyThrows
  private static String cascadingSerialize(Object object) {
    return HadoopUtil.serializeBase64(object, new JobConf(new Configuration()));
  }

  @SneakyThrows
  private static DistributedDocumentTask cascadingDeserialize(String task) {
    return HadoopUtil.deserializeBase64(task, new JobConf(new Configuration()), DistributedDocumentTask.class);
  }

}
