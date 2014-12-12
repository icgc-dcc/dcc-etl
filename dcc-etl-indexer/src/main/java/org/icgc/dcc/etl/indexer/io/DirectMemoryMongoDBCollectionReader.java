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
package org.icgc.dcc.etl.indexer.io;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.primitives.Ints.max;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_MUTATION_ID;
import static org.icgc.dcc.common.core.util.FormatUtils.formatBytes;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatMemory;
import static org.icgc.dcc.common.core.util.FormatUtils.formatRate;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getObservationConsequenceGeneIds;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getObservationDonorId;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getObservationMutationId;
import static org.icgc.dcc.etl.indexer.util.DirectMemoryUtils.getDirectMemorySize;

import java.io.IOException;
import java.util.Collection;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.serialization.Serializer;
import org.icgc.dcc.etl.indexer.model.CollectionFields;
import org.icgc.dcc.etl.indexer.util.SmileObjectNodeSerializer;
import org.jongo.Jongo;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Data access layer for document collection sources.
 * 
 * @deprecated until in-memory data structures can be improved.
 */
@Slf4j
@Deprecated
public class DirectMemoryMongoDBCollectionReader extends MongoDBCollectionReader {

  /**
   * Constants.
   */
  private static final int GB = 1000 * 1000 * 1000;

  /**
   * Cache configuration.
   */
  private static final Serializer CACHE_SERIALIZER = new SmileObjectNodeSerializer();
  private static final int CACHE_BUFFER_SIZE = 1 * GB;
  private static final float DIRECT_MEMORY_PERCENT = 0.6f;
  private static final int CACHE_INITIAL_CAPACITY = CACHE_BUFFER_SIZE;
  private static final int CACHE_CONCURRENCY_LEVEL = 1; // Only one thread will ever access
  private static final long CACHE_DISPOSAL_TIME = HOURS.toMillis(24); // Arbitrary

  /**
   * Off-heap second level cache.
   */
  private CacheService<Integer, ObjectNode> observationCache;

  /**
   * Indexes.
   */
  private static final Multimap<String, Integer> observationIdsByDonorId = newMultiMap();
  private static final Multimap<String, Integer> observationIdsByGeneId = newMultiMap();
  private static final Multimap<String, Integer> observationIdsByMutationId = newMultiMap();

  public DirectMemoryMongoDBCollectionReader(@NonNull Jongo client) {
    super(client);
  }

  @Override
  public Iterable<ObjectNode> readObservations(@NonNull CollectionFields fields) {
    return retrieve(getObservationIds(fields));
  }

  @Override
  public Iterable<ObjectNode> readObservationsByDonorId(@NonNull String donorId, CollectionFields fields) {
    return retrieve(fields, observationIdsByDonorId, donorId);
  }

  @Override
  public Iterable<ObjectNode> readObservationsByGeneId(@NonNull String geneId, @NonNull CollectionFields fields) {
    return retrieve(fields, observationIdsByGeneId, geneId);
  }

  @Override
  public Iterable<ObjectNode> readObservationsByMutationId(String mutationId, CollectionFields fields) {
    return retrieve(fields, observationIdsByMutationId, mutationId);
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (observationCache != null) {
      observationCache.close();
    }
  }

  private Collection<Integer> getObservationIds(CollectionFields fields) {
    ensureCached(fields);
    return observationCache.getMap().keySet();
  }

  private Iterable<ObjectNode> retrieve(CollectionFields fields, Multimap<String, Integer> index, String key) {
    ensureCached(fields);

    val observationIds = index.get(key);
    return retrieve(observationIds);
  }

  private Iterable<ObjectNode> retrieve(Iterable<Integer> observationIds) {
    return transform(observationIds, new Function<Integer, ObjectNode>() {

      @Override
      public ObjectNode apply(Integer observationId) {
        return observationCache.retrieve(observationId);
      }

    });
  }

  private void ensureCached(CollectionFields fields) {
    // Lazy load
    if (observationCache == null) {
      cacheObservations(fields);
    }
  }

  private void cacheObservations(CollectionFields fields) {
    log.info("Caching observations ({})...", formatMemory());
    observationCache = newCacheService();
    val watch = Stopwatch.createStarted();

    // Clients may not require the observation's donor_id, but it is required for cache indexing so we added here and
    // remove it downstream once it is no longer required
    val donorIdFieldName = OBSERVATION_DONOR_ID;
    val addDonorId = !fields.uses(donorIdFieldName);
    if (addDonorId) {
      fields = fields.with(donorIdFieldName);
    }

    // Same as above
    val mutationIdFieldName = OBSERVATION_MUTATION_ID;
    val addMutationId = !fields.uses(mutationIdFieldName);
    if (addMutationId) {
      fields = fields.with(mutationIdFieldName);
    }

    // Read from source
    val observations = super.readObservations(fields);

    // Cache warming
    int count = 0;
    for (val observation : observations) {
      val status = ++count % 100000 == 0;
      if (status) {
        log.info("Cached {} observations ({} docs/s) in {}",
            new Object[] { formatCount(count), formatRate(count, watch), watch });
      }

      val observationId = count;
      checkNotNull(
          // Cache observation
          observationCache.put(count, observation),

          "Observation could not be added to cache. Not enough space found to store '%s' in cache with %s used and %s capacity. Memory: %s",
          observation,
          formatBytes(observationCache.getMemoryManager().used()),
          formatBytes(observationCache.getMemoryManager().capacity()),
          formatMemory());

      // Cache donor observations
      val donorId = getObservationDonorId(observation);
      observationIdsByDonorId.put(donorId, observationId);
      if (addDonorId) {
        observation.remove(donorIdFieldName);
      }

      // Cache mutation observations
      val mutationId = getObservationMutationId(observation);
      observationIdsByMutationId.put(mutationId, observationId);
      if (addMutationId) {
        observation.remove(mutationIdFieldName);
      }

      // Cache gene observations
      val geneIds = getObservationConsequenceGeneIds(observation);
      for (val geneId : geneIds) {
        observationIdsByGeneId.put(geneId, observationId);
      }
    }

    log.info("Finished caching {} observations in cache with {} used and {} capacity in {}. Memory: {}", new Object[] {
        formatCount(count),
        formatBytes(observationCache.getMemoryManager().used()),
        formatBytes(observationCache.getMemoryManager().capacity()),
        watch,
        formatMemory()
    });
  }

  private static CacheService<Integer, ObjectNode> newCacheService() {
    log.info("Creating direct memory cache...");

    val directMemorySize = getDirectMemorySize();
    log.info("JVM: directMemorySize = {}", formatBytes(directMemorySize));

    val bufferCount = getBufferCount(directMemorySize);
    log.info(
        "Cache: directMemoryPercent = {}, bufferSize = {}, bufferCount = {}, intitialCapacity = {}, concurrencyLevel = {}",
        new Object[] {
            DIRECT_MEMORY_PERCENT,
            formatBytes(CACHE_BUFFER_SIZE),
            bufferCount,
            formatBytes(CACHE_INITIAL_CAPACITY),
            CACHE_CONCURRENCY_LEVEL });

    val cacheService = new DirectMemory<Integer, ObjectNode>()
        .setSerializer(CACHE_SERIALIZER)
        .setSize(CACHE_BUFFER_SIZE)
        .setNumberOfBuffers(bufferCount)
        .setConcurrencyLevel(CACHE_CONCURRENCY_LEVEL)
        .setDisposalTime(CACHE_DISPOSAL_TIME)
        .newCacheService();

    log.info("Finished creating direct memory cache");
    return cacheService;
  }

  private static int getBufferCount(long directMemorySize) {
    return max((int) (DIRECT_MEMORY_PERCENT * directMemorySize / CACHE_BUFFER_SIZE), 1);
  }

  private static Multimap<String, Integer> newMultiMap() {
    return HashMultimap.create();
  }

}
