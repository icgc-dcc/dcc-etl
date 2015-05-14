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
package org.icgc.dcc.etl.repo.core;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.icgc.dcc.common.core.model.ReleaseCollection.FILE_COLLECTION;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.Jackson.DEFAULT;
import static org.icgc.dcc.common.core.util.VersionUtils.getScmInfo;
import static org.icgc.dcc.etl.repo.core.RepositoryFileIndexer.RepositoryFileIndex.getSettings;
import static org.icgc.dcc.etl.repo.core.RepositoryFileIndexer.RepositoryFileIndex.isRepoIndex;
import static org.icgc.dcc.etl.repo.core.RepositoryFileIndexer.RepositoryFileIndex.repoDateDescending;
import static org.icgc.dcc.etl.repo.core.RepositoryFileIndexer.RepositoryFileIndex.resolveIndexName;
import static org.icgc.dcc.etl.repo.util.Collectors.toImmutableSet;
import static org.icgc.dcc.etl.repo.util.Streams.stream;
import static org.icgc.dcc.etl.repo.util.TransportClientFactory.newTransportClient;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.icgc.dcc.etl.repo.util.AbstractJongoComponent;
import org.joda.time.DateTime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientURI;

@Slf4j
public class RepositoryFileIndexer extends AbstractJongoComponent implements Closeable {

  /**
   * Index naming.
   */
  private static final String REPO_INDEX_ALIAS = "icgc-repository";
  private static final DateTimeFormatter INDEX_NAME_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

  /**
   * Index types.
   */
  private static final String INDEX_TYPE_NAME = "file";
  private static final String INDEX_TYPE_TEXT_NAME = "file-text";
  private static final List<String> INDEX_TYPE_NAMES = ImmutableList.of(INDEX_TYPE_NAME, INDEX_TYPE_TEXT_NAME);

  /**
   * Metadata location.
   */
  private static final String ES_CONFIG_BASE_PATH = "mappings";

  /**
   * Configuration.
   */
  @NonNull
  private final String indexName;

  /**
   * Dependencies.
   */
  @NonNull
  private final TransportClient client;

  public RepositoryFileIndexer(@NonNull MongoClientURI mongoUri, @NonNull String esUri) {
    super(mongoUri);
    this.indexName = resolveIndexName();
    this.client = newTransportClient(esUri);
  }

  public void indexFiles() {
    initializeIndex();
    indexDocuments();
    aliasIndex();
    pruneIndexes();
  }

  @Override
  public void close() throws IOException {
    client.close();
    super.close();
  }

  private void initializeIndex() {
    val indexClient = client.admin().indices();

    log.info("Checking index '{}' for existence...", indexName);
    val exists = indexClient.prepareExists(indexName)
        .execute()
        .actionGet()
        .isExists();

    if (exists) {
      log.info("Deleting index '{}'...", indexName);
      checkState(indexClient.prepareDelete(indexName)
          .execute()
          .actionGet()
          .isAcknowledged(),
          "Index '%s' deletion was not acknowledged", indexName);
    }

    try {
      log.info("Creating index '{}'...", indexName);
      checkState(indexClient
          .prepareCreate(indexName)
          .setSettings(getSettings().toString())
          .execute()
          .actionGet()
          .isAcknowledged(),
          "Index '%s' creation was not acknowledged!", indexName);

      for (val typeName : INDEX_TYPE_NAMES) {
        val source = getTypeMapping(typeName).toString();

        log.info("Creating index '{}' mapping for type '{}'...", indexName, typeName);
        checkState(indexClient.preparePutMapping(indexName)
            .setType(typeName)
            .setSource(source)
            .execute()
            .actionGet()
            .isAcknowledged(),
            "Index '%s' type mapping in index '%s' was not acknowledged for '%s'!",
            typeName, indexName);
      }
    } catch (Throwable t) {
      propagate(t);
    }
  }

  private void indexDocuments() {
    val watch = createStarted();
    log.info("Indexing documents...");
    @Cleanup
    val processor = createBulkProcessor();

    val documentCount = eachDocument(FILE_COLLECTION, file -> {
      // Need to remove this as to not conflict with Elasticsearch
        file.remove("_id");
        processor.add(indexRequest(indexName).type(INDEX_TYPE_NAME).source(file.toString()));

        JsonNode fileText = createFileText(file);
        processor.add(indexRequest(indexName).type(INDEX_TYPE_TEXT_NAME).source(fileText.toString()));
      });

    log.info("Finished indexing {} documents in {}", formatCount(documentCount), watch);
  }

  private BulkProcessor createBulkProcessor() {
    return BulkProcessor.builder(client, new Listener() {

      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        log.info("[{}] executing [{}]/[{}]", executionId, request.numberOfActions(),
            new ByteSizeValue(request.estimatedSizeInBytes()));
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        log.info("'{}' executed  [{}]/[{}], took {}", executionId, request.numberOfActions(), new ByteSizeValue(
            request.estimatedSizeInBytes()), response.getTook());

        checkState(!response.hasFailures(), "'%s' failed to execute bulk request: %s", executionId,
            response.buildFailureMessage());
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable e) {
        log.info("'{}' failed to execute bulk request", e, executionId);
      }

    }).build();
  }

  @SneakyThrows
  private void aliasIndex() {
    val alias = REPO_INDEX_ALIAS;

    // Remove existing alias
    val request = client.admin().indices().prepareAliases();
    for (val index : getIndexNames()) {
      request.removeAlias(index, alias);
    }

    // Add new alias
    log.info("Assigning index alias {} to index {}...", alias, indexName);
    request.addAlias(indexName, alias);

    // Re-assign
    checkState(request
        .execute()
        .actionGet()
        .isAcknowledged(),
        "Assigning index alias '%s' to index '%s' was not acknowledged!",
        alias, indexName);
  }

  private void pruneIndexes() {
    String[] staleRepoIndexNames =
        getIndexNames()
            .stream()
            .filter(isRepoIndex())
            .sorted(repoDateDescending())
            .skip(3) // Keep 3
            .toArray(size -> new String[size]);

    if (staleRepoIndexNames.length == 0) {
      return;
    }

    log.info("Pruning stale indexes '{}'...", Arrays.toString(staleRepoIndexNames));
    val indexClient = client.admin().indices();
    checkState(indexClient.prepareDelete(staleRepoIndexNames)
        .execute()
        .actionGet()
        .isAcknowledged(),
        "Index '%s' deletion was not acknowledged", Arrays.toString(staleRepoIndexNames));
  }

  private JsonNode createFileText(ObjectNode file) {
    val fileName = file.path("repository").path("file_name");
    val fileText = file.objectNode().set("file_name", fileName);

    return fileText;
  }

  private Set<String> getIndexNames() {
    val state = client.admin()
        .cluster()
        .prepareState()
        .execute()
        .actionGet()
        .getState();

    return stream(state.getMetaData().getIndices().keys())
        .map(key -> key.value)
        .collect(toImmutableSet());
  }

  private static ObjectNode getTypeMapping(String typeName) throws JsonProcessingException, IOException {
    val resourceName = format("%s/%s.mapping.json", ES_CONFIG_BASE_PATH, typeName);
    val mappingFileUrl = getResource(resourceName);
    val typeMapping = DEFAULT.readTree(mappingFileUrl);

    // Add meta data for index-to-indexer traceability
    val _meta = (ObjectNode) typeMapping.get(typeName).with("_meta");
    _meta.put("creation_date", DateTime.now().toString());
    for (val entry : getScmInfo().entrySet()) {
      val key = entry.getKey();
      val value = nullToEmpty(entry.getValue()).replaceAll("\n", " ");
      _meta.put(key, value);
    }

    return (ObjectNode) typeMapping;
  }

  public static class RepositoryFileIndex {

    public static String resolveIndexName() {
      val currentDate = INDEX_NAME_DATE_FORMAT.format(LocalDate.now());
      return REPO_INDEX_ALIAS + "-" + currentDate;
    }

    public static ObjectNode getSettings() throws IOException {
      val resourceName = format("%s/index.settings.json", ES_CONFIG_BASE_PATH);
      val settingsFileUrl = getResource(resourceName);

      return (ObjectNode) DEFAULT.readTree(settingsFileUrl);
    }

    public static LocalDate parseIndexTime(String indexName) {
      val date = indexName.replace(REPO_INDEX_ALIAS + "-", "");
      return INDEX_NAME_DATE_FORMAT.parse(date, LocalDate::from);
    }

    public static Predicate<? super String> isRepoIndex() {
      return indexName -> indexName.startsWith(REPO_INDEX_ALIAS);
    }

    public static Comparator<? super String> repoDateDescending() {
      return (repoIndexA, repoIndexB) -> {
        return -parseIndexTime(repoIndexA).compareTo(parseIndexTime(repoIndexB)); // Time descending
      };
    }

  }

}
