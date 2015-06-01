/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_EXPERIMENTAL_ANALYSIS_PERFORMED;
import static org.icgc.dcc.common.core.model.FieldNames.EXPERIMENTAL_ANALYSIS_PERFORMED_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.EXPERIMENTAL_ANALYSIS_PERFORMED_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY_REPOSITORY;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatRate;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.etl.summarizer.repository.ReleaseRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;

/**
 * Summarizes {@code Project} collection information.
 * <p>
 * Updates {@code Project} documents to be of the form:
 * 
 * <pre>
 * {
 *   ...
 *   "_summary" : {
 *     "_available_data_type" : [ "ssm", "cnsm" ],
 *     "_ssm1_tested_donor_count" : 1,
 *     "_cnsm_tested_donor_count" : 1,
 *     "_cngv_tested_donor_count" : 0,
 *     "_sgv_tested_donor_count" : 0,
 *     "_stgv_tested_donor_count" : 0,
 *     "_stsm_tested_donor_count" : 0,
 *     "_exp_seq_tested_donor_count" : 0,
 *     "_exp_array_tested_donor_count" : 0,
 *     "_meth_seq_tested_donor_count" : 0,
 *     "_meth_array_tested_donor_count" : 0,
 *     "_jcn_tested_donor_count" : 0,
 *     "_mirna_seq_tested_donor_count" : 0,
 *     "_pexp_tested_donor_count" : 0,
 *     "_total_donor_count" : 1,
 *     "_total_specimen_count" : 1,
 *     "_total_sample_count" : 1,
 *     "repository" : [ "EGA", "CGHub" ],     
 *     "experimental_analysis_performed" : {
 *       "WXS" : 1
 *     },     
 *   },
 *   ...
 * }
 * </pre>
 */
@Slf4j
public class ProjectSummarizer extends AbstractSummarizer {

  private static final int COUNTER_THRESHOLD = 10;

  public ProjectSummarizer(ReleaseRepository repository) {
    super(repository);
  }

  @Override
  public void summarize() {
    log.info("Finding  project ids...");
    List<String> projectIds = repository.getProjectIds();
    log.info("Found {} project ids", formatCount(projectIds.size()));

    // Retrieve all known projects and map projectId -> summary
    Map<String, ObjectNode> projectSummaries = createProjectSummaries(projectIds);

    // Summarize
    summarizeAvailableTypes(projectSummaries);
    summarizeTotalDonorCounts(projectSummaries);
    summarizeTotalCompleteDonorCounts(projectSummaries);
    summarizeSpecimenSampleCounts(projectSummaries);
    summarizeTestedTypeCounts(projectSummaries);
    summarizeRepositories(projectSummaries);
    summarizeLibraryStrategyCounts(projectSummaries);

    // Persist
    int count = 0;
    Stopwatch watch = Stopwatch.createStarted();
    for (Entry<String, ObjectNode> entry : projectSummaries.entrySet()) {
      String projectId = entry.getKey();
      ObjectNode projectSummary = entry.getValue();

      repository.setProjectSummary(projectId, projectSummary);

      if (++count % COUNTER_THRESHOLD == 0) {
        log.info("Summarized {} project(s) ({} docs/s)", //
            formatCount(count), formatRate(rate(COUNTER_THRESHOLD, watch)));
      }
    }
  }

  private Map<String, ObjectNode> createProjectSummaries(List<String> projectIds) {
    Map<String, ObjectNode> projectSummaries = newHashMap();
    for (String projectId : projectIds) {
      projectSummaries.put(projectId, createDefaultProjectSummary());
    }

    return projectSummaries;
  }

  private ObjectNode createDefaultProjectSummary() {
    ObjectNode projectSummary = createSummary();

    // Defaults
    for (FeatureType type : FeatureType.values()) {
      setTestedTypeCount(projectSummary, type, 0);
    }

    // Defaults
    setAvailableTypes(projectSummary, projectSummary.arrayNode());
    setTotalDonorCount(projectSummary, 0);
    setTotalSampleCount(projectSummary, 0);
    setTotalSpecimenCount(projectSummary, 0);

    return projectSummary;
  }

  private void summarizeTestedTypeCounts(Map<String, ObjectNode> projectSummaries) {
    // Summarize and set the total number of each observation type in each project
    for (FeatureType type : FeatureType.values()) {
      List<JsonNode> results = repository.getProjectObservationTypeCount(type);

      for (JsonNode result : results) {
        String projectId = result.get("projectId").textValue();
        int typeCount = result.get("typeCount").asInt();

        ObjectNode projectSummary = projectSummaries.get(projectId);
        setTestedTypeCount(projectSummary, type, typeCount);
      }
    }
  }

  private void summarizeSpecimenSampleCounts(Map<String, ObjectNode> projectSummaries) {
    // Summarize and set the total number of samples and specimens in each project
    List<JsonNode> results = repository.getProjectSampleSpecimenCounts();

    for (JsonNode result : results) {
      String projectId = result.get("projectId").textValue();
      int sampleCount = result.get("sampleCount").asInt();
      int specimenCount = result.get("specimenCount").asInt();

      ObjectNode projectSummary = projectSummaries.get(projectId);
      setTotalSampleCount(projectSummary, sampleCount);
      setTotalSpecimenCount(projectSummary, specimenCount);
    }
  }

  private void summarizeTotalDonorCounts(Map<String, ObjectNode> projectSummaries) {
    // Summarize and set the total number of donors in each project
    List<JsonNode> results = repository.getProjectDonorCounts();

    for (JsonNode result : results) {
      String projectId = result.get("projectId").textValue();
      int donorCount = result.get("donorCount").asInt();

      ObjectNode projectSummary = projectSummaries.get(projectId);
      setTotalDonorCount(projectSummary, donorCount);
    }
  }

  private void summarizeTotalCompleteDonorCounts(Map<String, ObjectNode> projectSummaries) {
    val results =
        uniqueIndex(repository.getProjectCompleteDonorCounts(), result -> result.get("projectId").textValue());

    for (val entry : projectSummaries.entrySet()) {
      val projectId = entry.getKey();
      val projectSummary = entry.getValue();

      val result = results.get(projectId);
      val donorCount = result != null ? result.get("donorCount").asInt() : 0;
      val complete = donorCount > 0;

      setComplete(projectSummary, complete);
      setTotalCompleteDonorCount(projectSummary, donorCount);
    }
  }

  private void summarizeAvailableTypes(Map<String, ObjectNode> projectSummaries) {
    // Summarize and set the unique donor observation types belonging to each project
    List<JsonNode> results = repository.getProjectObservationTypes();

    for (JsonNode result : results) {
      String projectId = result.get("projectId").textValue();
      JsonNode types = result.get("types");

      ObjectNode projectSummary = projectSummaries.get(projectId);
      checkState(projectSummary != null, "No project summary for project id '%s' in %s", //
          projectId, projectSummaries.keySet());

      setAvailableTypes(projectSummary, types);
    }
  }

  private void summarizeRepositories(Map<String, ObjectNode> projectSummaries) {
    // Summarize and set the unique donor repositories belonging to each project
    List<JsonNode> results = repository.getProjectRepositories();

    for (JsonNode result : results) {
      String projectId = result.get("projectId").textValue();
      JsonNode repositories = result.get("repositories");

      ObjectNode projectSummary = projectSummaries.get(projectId);
      checkState(projectSummary != null, "No project summary for project id '%s' in %s", //
          projectId, projectSummaries.keySet());

      projectSummary.set(PROJECT_SUMMARY_REPOSITORY, repositories);
    }
  }

  private void summarizeLibraryStrategyCounts(Map<String, ObjectNode> projectSummaries) {
    // Summarize and set the distinct number of donor library strategies / samples in each project
    val results = repository.getProjectLibraryStrategySampleCounts();

    for (val result : results) {
      String projectId = result.get("projectId").textValue();
      JsonNode sampleCounts = result.get("sampleCounts");

      Map<String, Long> donorCountSummary = newHashMap();
      Map<String, Long> sampleCountSummary = newHashMap();
      Set<String> uniqueLibraryStrategy = newLinkedHashSet();

      for (JsonNode record : sampleCounts) {
        val iterator = record.fields();
        while (iterator.hasNext()) {
          // Shorthands
          val entry = iterator.next();
          val libraryStategy = entry.getKey();
          val donorSampleCount = entry.getValue().longValue();

          // Increment donor counts
          Long donorCount = donorCountSummary.get(libraryStategy);
          donorCount = firstNonNull(donorCount, 0L) + 1;
          donorCountSummary.put(libraryStategy, donorCount);

          // Increment sample counts
          Long sampleCount = sampleCountSummary.get(libraryStategy);
          sampleCount = firstNonNull(sampleCount, 0L) + donorSampleCount;
          sampleCountSummary.put(libraryStategy, sampleCount);

          uniqueLibraryStrategy.add(libraryStategy);
        }
      }

      ObjectNode projectSummary = projectSummaries.get(projectId);
      projectSummary.putPOJO(EXPERIMENTAL_ANALYSIS_PERFORMED_DONOR_COUNT, donorCountSummary);
      projectSummary.putPOJO(EXPERIMENTAL_ANALYSIS_PERFORMED_SAMPLE_COUNT, sampleCountSummary);

      // Create an array with just the names
      projectSummary.putPOJO(AVAILABLE_EXPERIMENTAL_ANALYSIS_PERFORMED, uniqueLibraryStrategy.toArray());

    }
  }
}
