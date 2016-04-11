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

import static com.google.common.collect.Maps.filterEntries;
import static com.google.common.collect.Sets.newTreeSet;
import static org.icgc.dcc.common.core.model.FieldNames.AFFECTED_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONORS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECTS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.common.core.util.Formats.formatRate;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.etl.summarizer.repository.ReleaseRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * Summarizes {@code Gene} collection information.
 * <p>
 * Updates {@code Gene} documents to be of the form:
 * 
 * <pre>
 * {
 *  ...
 *   "donor" : [ {
 *     "_donor_id" : "donor1",
 *     "_summary" : {
 *       "_ssm_count" : 2,
 *       "_cnsm_exists" : true,
 *       "_cngv_exists" : false,
 *       "_sgv_exists" : false,
 *       "_stgv_exists" : false,
 *       "_stsm_exists" : false,
 *       "_exp_seq_exists" : false,
 *       "_exp_array_exists" : false,
 *       "_meth_seq_exists" : false,
 *       "_meth_array_exists" : false,
 *       "_jcn_exists" : false,
 *       "_mirna_seq_exists" : false,
 *       "_pexp_exists" : false
 *     }
 *   } ],
 *   "project" : [ {
 *     "_project_id" : "project1",
 *     "_summary" : {
 *       "_affected_donor_count" : 1,
 *       "_available_data_type" : [ "ssm", "cnsm" ]
 *     }
 *   } ],
 *   ...
 * }
 * </pre>
 */
@Slf4j
public class GeneSummarizer extends AbstractSummarizer {

  private static final int COUNTER_THRESHOLD = 10000;

  private int processingCount = 0;
  private int persistanceCount = 0;
  private final Stopwatch processingWatch = Stopwatch.createStarted();
  private final Stopwatch persistanceWatch = Stopwatch.createStarted();

  public GeneSummarizer(ReleaseRepository repository) {
    super(repository);
  }

  @Override
  public void summarize() {
    log.info("Finding donor-project mapping...");
    val donorProjectIds = repository.getDonorProjectIds();
    log.info("Found {} donor-project mappings", formatCount(donorProjectIds.size()));

    val observations = repository.getDonorGeneObservationTypes();

    val table = HashBasedTable.<String, String, Map<FeatureType, Short>> create();
    for (val observation : observations) {
      try {
        updateTableCounts(observation, table);
      } catch (Exception e) {
        log.error(String.format("Error processing observation: '{}'", observation), e);
        throw new RuntimeException(e);
      }
    }

    log.info("Gathering results ('{}' cells)", table.size());
    for (val geneId : table.rowKeySet()) {
      repository.setGeneSummary(
          geneId,
          createGeneEntry(geneId, donorProjectIds, table));
    }
  }

  private void updateTableCounts(JsonNode observation, HashBasedTable<String, String, Map<FeatureType, Short>> table) {

    val donorId = STRING_INTERNER.intern(observation.get(OBSERVATION_DONOR_ID).asText());
    val type = FeatureType.from(STRING_INTERNER.intern(observation.get(OBSERVATION_TYPE).asText()));
    val uniqueGeneIds = getUniqueGeneIds(observation.path(OBSERVATION_CONSEQUENCES), false);

    for (val geneId : uniqueGeneIds) {
      Map<FeatureType, Short> countMap = table.get(geneId, donorId);
      if (countMap == null) {
        countMap = createDefaultCountMap();
        table.put(geneId, donorId, countMap);
        if (++processingCount % COUNTER_THRESHOLD == 0) {
          log.info("Pre-processed {} gene-donor(s) ({} docs/s)",
              formatCount(processingCount), formatRate(processingCount, processingWatch));
        }
      }
      countMap.put(type, (short) (countMap.get(type) + 1));
    }
  }

  private ObjectNode createGeneEntry(
      String geneId,
      Map<String, String> donorProjectIds,
      HashBasedTable<String, String, Map<FeatureType, Short>> table) {
    val row = table.row(geneId);

    val projectToDonors = Maps.<String, Integer> newHashMap();
    val projectAvailableTypes = ArrayListMultimap.<String, FeatureType> create();

    val donorArray = createArray();
    for (val donorId : row.keySet()) {

      val countMap = row.get(donorId);
      donorArray.add(createDonorEntry(donorId, countMap));

      val projectId = donorProjectIds.get(donorId);
      {
        val currentCount = projectToDonors.get(projectId);
        projectToDonors.put(projectId, currentCount == null ? 1 : currentCount + 1);
      }

      projectAvailableTypes.putAll(
          projectId,
          filterEntries(countMap, new Predicate<Entry<FeatureType, Short>>() {

            @Override
            public boolean apply(Entry<FeatureType, Short> entry) {
              return entry.getValue() > 0;
            }

          }).keySet());

      if (++persistanceCount % COUNTER_THRESHOLD == 0) {
        log.info("Summarized {} gene-donor(s) ({} docs/s)",
            formatCount(persistanceCount), formatRate(persistanceCount, persistanceWatch));
      }
    }

    val geneEntry = createObject();
    geneEntry.put(GENE_DONORS, donorArray);
    geneEntry.put(GENE_PROJECTS, createProjectArray(projectToDonors, projectAvailableTypes));

    return geneEntry;
  }

  private ObjectNode createDonorEntry(String donorId, Map<FeatureType, Short> countMap) {
    val donorEntry = createObject();
    donorEntry.put(GENE_DONOR_DONOR_ID, donorId);
    donorEntry.put(GENE_DONOR_SUMMARY, toTypeSummaryObjectNode(countMap));
    return donorEntry;
  }

  private ArrayNode createProjectArray(
      HashMap<String, Integer> projectToDonors,
      ArrayListMultimap<String, FeatureType> projectAvailableTypes) {
    val projectArray = createArray();
    for (val projectId : projectToDonors.keySet()) {
      projectArray.add(
          createProjectEntry(
              projectId,
              projectToDonors,
              projectAvailableTypes));
    }
    return projectArray;
  }

  private ObjectNode createProjectEntry(
      String projectId,
      HashMap<String, Integer> projectToDonors,
      Multimap<String, FeatureType> projectAvailableTypes) {
    val projectEntry = createObject();
    projectEntry.put(GENE_PROJECT_PROJECT_ID, projectId);

    val summaryEntry = projectEntry.with(GENE_PROJECT_SUMMARY);

    val donorCount = projectToDonors.get(projectId);
    summaryEntry.put(AFFECTED_DONOR_COUNT, donorCount);

    val availableTypes = summaryEntry.withArray(AVAILABLE_DATA_TYPES);
    for (val type : newTreeSet(projectAvailableTypes.get(projectId))) {
      availableTypes.add(type.getId());
    }
    return projectEntry;
  }

  private static Map<FeatureType, Short> createDefaultCountMap() {
    val countMap = Maps.<FeatureType, Short> newHashMap();
    for (val type : FeatureType.values()) {
      countMap.put(type, (short) 0);
    }
    return countMap;
  }

  private static ObjectNode toTypeSummaryObjectNode(Map<FeatureType, Short> countMap) {
    val summary = createSummary();
    for (val entry : countMap.entrySet()) {
      setTypeMetric(summary, entry.getKey(), entry.getValue());
    }
    return summary;
  }

}
