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
package org.icgc.dcc.etl.repo.index;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.propagate;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.icgc.dcc.common.core.model.ReleaseCollection.FILE_COLLECTION;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.Jackson.DEFAULT;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.etl.repo.index.RepositoryFileIndex.INDEX_TYPE_FILE_DONOR_TEXT_NAME;
import static org.icgc.dcc.etl.repo.index.RepositoryFileIndex.INDEX_TYPE_FILE_NAME;
import static org.icgc.dcc.etl.repo.index.RepositoryFileIndex.INDEX_TYPE_FILE_TEXT_NAME;
import static org.icgc.dcc.etl.repo.index.RepositoryFileIndex.INDEX_TYPE_NAMES;
import static org.icgc.dcc.etl.repo.index.RepositoryFileIndex.REPO_INDEX_ALIAS;
import static org.icgc.dcc.etl.repo.index.RepositoryFileIndex.compareIndexDateDescending;
import static org.icgc.dcc.etl.repo.index.RepositoryFileIndex.getCurrentIndexName;
import static org.icgc.dcc.etl.repo.index.RepositoryFileIndex.getSettings;
import static org.icgc.dcc.etl.repo.index.RepositoryFileIndex.getTypeMapping;
import static org.icgc.dcc.etl.repo.index.RepositoryFileIndex.isRepoIndexName;
import static org.icgc.dcc.etl.repo.util.TransportClientFactory.newTransportClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

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
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.icgc.dcc.etl.repo.util.AbstractJongoComponent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.mongodb.MongoClientURI;

@Slf4j
public class RepositoryFileIndexer extends AbstractJongoComponent implements Closeable {

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
    this.indexName = getCurrentIndexName();
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

    @Cleanup
    val processor = createBulkProcessor();

    log.info("Indexing file documents...");
    val fileCount = indexFileDocuments(processor);
    log.info("Indexing file donor documents...");
    val fileDonorCount = indexFileDonorDocuments(processor);

    log.info("Finished indexing {} file and {} documents in {}", formatCount(fileCount), formatCount(fileDonorCount),
        watch);
  }

  private int indexFileDocuments(BulkProcessor processor) {
    return eachDocument(FILE_COLLECTION, file -> {
      String id = file.get("id").textValue();

      // Need to remove this as to not conflict with Elasticsearch
        file.remove("_id");
        processor.add(indexRequest(indexName).type(INDEX_TYPE_FILE_NAME).id(id).source(file.toString()));

        JsonNode fileText = createFileText(file, id);
        processor.add(indexRequest(indexName).type(INDEX_TYPE_FILE_TEXT_NAME).id(id).source(fileText.toString()));
      });
  }

  @SneakyThrows
  private int indexFileDonorDocuments(BulkProcessor processor) {
    val fieldNames = ImmutableList.of(
        "specimen_id",
        "sample_id",
        "submitted_donor_id",
        "submitted_specimen_id",
        "submitted_sample_id",
        "tcga_participant_barcode",
        "tcga_sample_barcode",
        "tcga_aliquot_barcode");

    val donorIds = Sets.<String> newHashSet();
    val donorFields = Maps.<String, Multimap<String, String>> newHashMap();
    for (val fieldName : fieldNames) {
      donorFields.put(fieldName, HashMultimap.<String, String> create());
    }

    eachDocument(FILE_COLLECTION, file -> {
      JsonNode donor = file.path("donor");
      String donorId = donor.get("donor_id").textValue();
      donorIds.add(donorId);

      for (String fieldName : fieldNames) {
        String value = donor.get(fieldName).textValue();
        if (!isNullOrEmpty(value)) {
          donorFields.get(fieldName).put(donorId, value);
        }
      }
    });

    for (val donorId : donorIds) {
      val fileDonor = DEFAULT.createObjectNode();

      // By convention this is called id
      fileDonor.put("id", donorId);
      for (val fieldName : fieldNames) {
        fileDonor.putPOJO(fieldName, donorFields.get(fieldName).get(donorId));
      }

      processor.add(indexRequest(indexName).type(INDEX_TYPE_FILE_DONOR_TEXT_NAME).id(donorId)
          .source(DEFAULT.writeValueAsString(fileDonor)));
    }

    return donorIds.size();
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
            .filter(isRepoIndexName())
            .sorted(compareIndexDateDescending())
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

  private JsonNode createFileText(ObjectNode file, String id) {
    val fileName = file.path("repository").path("file_name");
    val donorId = file.path("donor").path("donor_id");

    val fileText = DEFAULT.createObjectNode();
    fileText.put("type", "file");
    fileText.put("id", id);
    fileText.put("file_name", fileName);
    fileText.put("donor_id", donorId);

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

}
