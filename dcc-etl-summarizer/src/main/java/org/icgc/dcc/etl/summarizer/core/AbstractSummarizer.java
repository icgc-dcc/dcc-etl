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
package org.icgc.dcc.etl.summarizer.core;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Strings.emptyToNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY_STATE;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_LIVE_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SPECIMEN_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.getTestedTypeCountFieldName;

import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.etl.summarizer.repository.ReleaseRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Sets;

/**
 * Abstract base class for the collection summarizer implementations. Provides utility methods and objects.
 */
@RequiredArgsConstructor
public abstract class AbstractSummarizer {

  /**
   * ID of "fake" gene
   */
  public static final String FAKE_GENE_ID = "";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final Interner<String> STRING_INTERNER = Interners.newWeakInterner();

  final ReleaseRepository repository;

  /**
   * Summarizes collection information.
   */
  public abstract void summarize();

  float rate(int count, Stopwatch watch) {
    long seconds = watch.elapsed(SECONDS);
    float rate = seconds == 0 ? 0.0f : count / (float) seconds;
    watch.reset().start();

    return rate;
  }

  static ObjectNode createSummary() {
    return createObject();
  }

  static ObjectNode createObject() {
    return MAPPER.createObjectNode();
  }

  static ArrayNode createArray() {
    return MAPPER.createArrayNode();
  }

  static void addAvailableType(ObjectNode node, String type) {
    node.withArray(AVAILABLE_DATA_TYPES).add(type);
  }

  static void setAvailableTypes(ObjectNode node, JsonNode types) {
    node.put(AVAILABLE_DATA_TYPES, types);
  }

  static void setTypeMetric(ObjectNode node, FeatureType type, int count) {
    if (type.isSsm()) {
      setTypeCount(node, type, count);
    } else {
      setTypeExists(node, type, count != 0);
    }
  }

  static void setDefaultTypeMetrics(ObjectNode summary) {
    for (val type : FeatureType.values()) {
      setTypeMetric(summary, type, 0);
    }
  }

  private static void setTypeCount(ObjectNode node, FeatureType type, int count) {
    node.put(type.getSummaryFieldName(), count);
  }

  private static void setTypeExists(ObjectNode node, FeatureType type, boolean exists) {
    node.put(type.getSummaryFieldName(), exists);
  }

  static void setTestedTypeCount(ObjectNode node, FeatureType type, int count) {
    node.put(getTestedTypeCountFieldName(type), count);
  }

  static void setTotalDonorCount(ObjectNode node, int donorCount) {
    node.put(TOTAL_DONOR_COUNT, donorCount);
  }

  static void setProjectState(ObjectNode node, String state) {
    node.put(PROJECT_SUMMARY_STATE, state);
  }

  static void setTotalLiveDonorCount(ObjectNode node, int donorCount) {
    node.put(TOTAL_LIVE_DONOR_COUNT, donorCount);
  }

  static void setTotalSpecimenCount(ObjectNode node, int specimenCount) {
    node.put(TOTAL_SPECIMEN_COUNT, specimenCount);
  }

  static void setTotalSampleCount(ObjectNode node, int sampleCount) {
    node.put(TOTAL_SAMPLE_COUNT, sampleCount);
  }

  static Set<String> getUniqueGeneIds(JsonNode consequences, boolean includeFakeGene) {
    val uniqueGeneIds = Sets.<String> newTreeSet();
    for (val consequence : consequences) {
      val geneId = getConsequenceGeneId(consequence);

      if (!isFilteredGeneId(geneId) && (includeFakeGene || !FAKE_GENE_ID.equals(geneId))) {
        uniqueGeneIds.add(geneId);
      }
    }

    return uniqueGeneIds;
  }

  private static boolean isFilteredGeneId(@NonNull String geneId) {
    return "-999".equals(geneId) || "-888".equals(geneId) || "-777".equals(geneId);
  }

  private static String getConsequenceGeneId(@NonNull JsonNode consequence) {
    return STRING_INTERNER.intern(firstNonNull(
        emptyToNull(consequence.path(OBSERVATION_CONSEQUENCES_GENE_ID).textValue()),
        FAKE_GENE_ID));
  }

}
