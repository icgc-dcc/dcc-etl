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
package org.icgc.dcc.etl.indexer.hadoop;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.common.core.util.Formats.formatBytes;
import static org.icgc.dcc.common.core.util.Formats.formatCount;

import java.io.IOException;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

/**
 * ElasticSearch record writer.
 */
@Slf4j
public class EsRecordWriter implements RecordWriter<String, Object> {

  static final JsonFactory FACTORY = new SmileFactory();

  static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  static final ObjectWriter WRITER = MAPPER.writerWithType(ObjectNode.class);

  private final Progressable progress;

  private final Client client;

  private final int bulkSize;

  private final String indexName;

  private final String type;

  private BulkRequestBuilder bulk;

  EsRecordWriter(JobConf conf, Progressable progress) {
    // Configuration
    EsConfig config = new EsConfig(conf);
    this.bulkSize = config.getBulkSize();
    this.indexName = config.getIndex();
    this.type = config.getType();

    // Callback
    this.progress = progress;

    // State
    this.client = createClient(config);
    this.bulk = createBulk();
  }

  @Override
  public void write(String id, Object document) throws IOException {
    if (document == null) {
      log.warn("Null document for id '{}'", id);
      return;
    }

    indexDocument(id, document);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    // Flush
    sendBulk();

    // Cleanup
    client.close();
  }

  private TransportClient createClient(EsConfig config) {
    TransportClient client = new TransportClient();
    for (TransportAddress address : config.getTransportAddresses()) {
      client.addTransportAddress(address);
    }

    return client;
  }

  private void indexDocument(String id, Object document) throws IOException {
    IndexRequest request = createRequest(id, document);
    bufferRequest(request);

    // Report progress to Hadoop framework
    progress.progress();
  }

  private void bufferRequest(IndexRequest request) throws IOException {
    bulk.add(request);

    if (isBulkFull()) {
      sendBulk();
    }
  }

  private boolean isBulkFull() {
    return bulk.numberOfActions() >= bulkSize;
  }

  @SneakyThrows
  private IndexRequest createRequest(String id, Object document) {
    boolean json = false;
    byte[] bytes = json ? serializeJson(document) : serializeSmile(document);
    XContentType contentType = json ? JSON : SMILE;

    return createRequest(id, bytes, contentType);
  }

  private IndexRequest createRequest(String id, byte[] bytes, XContentType contentType) {
    return new IndexRequestBuilder(client)
        .setIndex(indexName)
        .setType(type)
        .setId(id)
        .setContentType(contentType)
        .setSource(bytes)
        .request();
  }

  private BulkRequestBuilder createBulk() {
    return this.client.prepareBulk();
  }

  private void sendBulk() throws IOException {
    int count = bulk.numberOfActions();
    if (count == 0) {
      return;
    }

    executeBulk();

    // Prepare for the next batch
    this.bulk = createBulk();
  }

  private void executeBulk() throws IOException {
    try {
      logBulk();
      verifyBulk(bulk.execute().actionGet());

      // Report progress to Hadoop framework
      progress.progress();
    } catch (Exception e) {
      log.error("Bulk request failure", e);
      throw new IOException(e);
    }
  }

  private void logBulk() {
    int count = bulk.numberOfActions();
    long bytes = bulk.request().estimatedSizeInBytes();

    log.info("Sending '{}' bulk request with {} items ({} bytes)",
        new Object[] { type, formatCount(count), formatBytes(bytes) });
  }

  private BulkResponse verifyBulk(BulkResponse response) {
    if (response.hasFailures()) {
      for (BulkItemResponse item : response) {
        checkState(item.isFailed() == false, "Failed to index es://%s/%s/%s because %s",
            item.getIndex(), item.getType(), item.getId(), item.getFailureMessage());
      }
    }

    return response;
  }

  private static byte[] serializeSmile(Object document) throws JsonProcessingException {
    return WRITER.writeValueAsBytes(document);
  }

  private static byte[] serializeJson(Object document) {
    return document.toString().getBytes(UTF_8);
  }

}
