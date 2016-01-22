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
