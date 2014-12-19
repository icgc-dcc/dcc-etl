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
package org.icgc.dcc.etl.indexer.schema;

import static com.google.common.base.Strings.repeat;
import static com.google.common.io.Files.createParentDirs;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.action.search.SearchType.SCAN;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;

import java.io.File;
import java.io.IOException;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.icgc.dcc.etl.indexer.core.Document;
import org.icgc.dcc.etl.indexer.model.DocumentType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;

/**
 * Reverse engineers the schema from Elasticsearch.
 */
@Slf4j
@RequiredArgsConstructor
public class ElasticSearchReverseSchemaGenerator {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int BATCH_SIZE = 10;
  private static final int TIME_OUT = 60000;
  private static final TimeValue KEEP_ALIVE = new TimeValue(TIME_OUT);

  /**
   * Configuration.
   */
  @NonNull
  private final String esUri;
  @NonNull
  private final String indexName;
  private final int limit;

  @SneakyThrows
  public void generate(DocumentType type, SchemaWriter writer) {
    val client = newTransportClient(esUri);
    val typeName = type.getName();

    SearchRequestBuilder search = client
        .prepareSearch(indexName)
        .setTypes(typeName)
        .setSearchType(SCAN)
        .setScroll(KEEP_ALIVE)
        .setQuery(matchAllQuery())
        .setSize(BATCH_SIZE);

    SearchResponse response = search
        .execute()
        .actionGet();

    int requestCount = 0;
    int count = 0;

    while (true) {
      val scroll = client
          .prepareSearchScroll(response.getScrollId())
          .setScroll(KEEP_ALIVE);

      log.info("Issuing scroll request #{} (0-based)", requestCount++);
      response = scroll
          .execute()
          .actionGet();

      // Break condition: No hits are returned
      val length = response.getHits().hits().length;
      if (count > limit || length == 0) {
        break;
      }

      for (val hit : response.getHits().hits()) {
        val source = convert(hit);
        writer.write(new Document(type, "", source));
        count++;
      }

      // Be nice to ES
      sleepUninterruptibly(1, SECONDS);
    }
  }

  @SneakyThrows
  private ObjectNode convert(SearchHit hit) {
    val source = hit.getSourceAsString();
    return MAPPER.readValue(source, ObjectNode.class);
  }

  private static File prepareSchemaFile(File schemaDir, DocumentType type) throws IOException {
    val schemaFile = new File(schemaDir, type.getName() + ".schema");
    schemaFile.delete();
    if (!schemaDir.exists()) {
      createParentDirs(schemaDir);
    }

    return schemaFile;
  }

  /**
   * Utility to reverse engineer all index type schemata from Elasticsearch.
   */
  @SneakyThrows
  public static void main(String[] args) {
    // Config
    int i = 0;
    val schemaDir = new File(args.length < ++i ? "target" : args[i - 1]);
    val esUri = args.length < ++i ? "es://***REMOVED***.oicr.on.ca:9300" : args[i - 1];
    val indexName = args.length < ++i ? "test17-trim" : args[i - 1];
    val limit = args.length < ++i ? 100 : Integer.valueOf(args[i - 1]);

    val watch = Stopwatch.createStarted();
    val generator = new ElasticSearchReverseSchemaGenerator(esUri, indexName, limit);
    for (val type : DocumentType.values()) {
      log.info("{}", repeat("-", 80));
      log.info("Processing '{}' document type...", type);
      log.info("{}", repeat("-", 80));

      val schemaFile = prepareSchemaFile(schemaDir, type);

      @Cleanup
      val writer = new SchemaWriter(schemaFile);

      generator.generate(type, writer);
    }

    log.info("Finished processing in {}", watch);
  }

}
