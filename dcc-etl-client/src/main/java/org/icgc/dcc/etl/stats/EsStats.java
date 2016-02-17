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
package org.icgc.dcc.etl.stats;

import static com.google.common.base.Joiner.on;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.action.search.SearchType.SCAN;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.MUTATION_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.OBSERVATION_CENTRIC_TYPE;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Gathers statistics about elasticsearch entities.
 */
@Slf4j
public class EsStats {

  private static final int BATCH_SIZE = 10000;
  private static final int TIME_OUT = 60000;
  private static final TimeValue KEEP_ALIVE = new TimeValue(TIME_OUT);

  private final String uri;

  private final String indexName;

  private final TransportClient client;

  EsStats(String uri, String indexName) {
    this.uri = checkNotNull(uri);
    this.indexName = indexName;

    this.client = transportClient();

    log.info("Using uri: {}, index {}", uri, indexName);
  }

  void stats(StatsResults results) {

    // Donors
    results.esDonorCount = client
        .prepareCount(indexName)
        .setTypes(DONOR_CENTRIC_TYPE.getName())
        .execute()
        .actionGet()
        .getCount();

    // Observations (order matters)
    results.esObservationCount = client
        .prepareCount(indexName)
        .setTypes(OBSERVATION_CENTRIC_TYPE.getName())
        .execute()
        .actionGet()
        .getCount();

    // Donors within observations (order matters)
    results.esObservationDonorCount = countUniqueBatch(
        results,
        indexName,
        OBSERVATION_CENTRIC_TYPE.getName(),
        nestedField(donorArray(), DONOR_ID));

    // Mutations
    results.esMutationCount = client
        .prepareCount(indexName)
        .setTypes(MUTATION_CENTRIC_TYPE.getName())
        .execute()
        .actionGet()
        .getCount();

    client.close();
  }

  private long countUniqueBatch(StatsResults results, String indexName, String typeName, String fieldName) {
    SearchRequestBuilder search =
        client
            .prepareSearch(indexName)
            .setTypes(typeName)
            .setSearchType(SCAN)
            .setScroll(KEEP_ALIVE)
            .addField(fieldName)
            .setQuery(matchAllQuery())
            .setSize(BATCH_SIZE);
    log.info("Issuing search request: '{}'", concise(search));

    SearchResponse response =
        search
            .execute()
            .actionGet();

    Set<String> set = newHashSet();
    int count = 0;
    while (true) {
      SearchScrollRequestBuilder scroll =
          client
              .prepareSearchScroll(
                  response.getScrollId())
              .setScroll(KEEP_ALIVE);

      log.info("Issuing scroll request #{} (0-based)", count++);
      response =
          scroll
              .execute()
              .actionGet();

      // Break condition: No hits are returned
      if (response.getHits().hits().length == 0) {
        break;
      }

      set.addAll(countUnique(response));
    }

    return set.size();
  }

  private Set<String> countUnique(SearchResponse response) {
    Set<String> set = newHashSet();
    for (SearchHit hit : response.getHits()) {
      set.add(getValue(hit.fields()));
    }
    return set;
  }

  private String getValue(Map<String, SearchHitField> fields) {
    checkState(fields.size() == 1); // Because we only projected one field
    return newArrayList(fields.values())
        .get(0)
        .getValue()
        .toString();
  }

  private String donorArray() {
    return "donor"; // Hardcoded in the indexer for now
  }

  private String nestedField(String... parts) {
    return on(".").join(parts);
  }

  private String concise(SearchRequestBuilder search) {
    return search.toString()
        .replaceAll("\n *", "");
  }

  @SneakyThrows
  @SuppressWarnings("resource")
  private TransportClient transportClient() {
    URI esUri = new URI(uri);
    String host = esUri.getHost();
    int port = esUri.getPort();
    return new TransportClient()
        .addTransportAddress(new InetSocketTransportAddress(host, port));
  }

}
