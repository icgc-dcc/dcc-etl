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
import static com.google.common.base.Strings.nullToEmpty;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatRate;
import static org.icgc.dcc.etl.summarizer.util.Donors.getAgeGroup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.summarizer.repository.ReleaseRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

/**
 * Summarizes {@code Donor} collection information.
 * <p>
 * Updates {@code Donor} documents to be of the form:
 * 
 * <pre>
 * {
 *   ...
 *   "_summary" : {
 *     "_available_data_type" : [ "ssm", "cnsm" ], // Created by loader
 *     "_ssm_count" : 1, // Created by loader
 *     "_cnsm_exists" : true, // Created by loader
 *     "_cngv_exists" : false, // Created by loader
 *     "_sgv_exists" : false, // Created by loader
 *     "_stgv_exists" : false, // Created by loader
 *     "_stsm_exists" : 0, // Created by loader
 *     "_exp_seq_exists" : false, // Created by loader
 *     "_exp_array_exists" : false, // Created by loader
 *     "_meth_seq_exists" : false, // Created by loader
 *     "_meth_array_exists" : false, // Created by loader
 *     "_jcn_exists" : false, // Created by loader
 *     "_mirna_seq_exists" : false, // Created by loader
 *     "_pexp_exists" : false, // Created by loader
 *     "_stsm_exists" : false, // Created by loader
 *     
 *     "_age_at_diagnosis_group": "1 - 9",
 *     "repository" : [ "EGA", "CGHub" ],   
 *     "experimental_analysis_performed" : [ "WXS" ]    
 *   },
 *   "gene" : [ {
 *     "_gene_id" : "",
 *     "_summary" : {
 *       "_ssm_count" : 0
 *     }
 *   }, {
 *     "_gene_id" : "gene3",
 *     "_summary" : {
 *       "_ssm_count" : 0
 *     }
 *   }, {
 *     "_gene_id" : "gene2",
 *     "_summary" : {
 *       "_ssm_count" : 1
 *     }
 *   }, {
 *     "_gene_id" : "gene1",
 *     "_summary" : {
 *       "_ssm_count" : 1
 *     }
 *   } ],
 *   ...
 * }
 * </pre>
 */
@Slf4j
public class DonorSummarizer extends AbstractSummarizer {

  /**
   * Constants.
   */
  private static final int COUNTER_THRESHOLD = 10;

  public DonorSummarizer(ReleaseRepository repository) {
    super(repository);
  }

  @Override
  public void summarize() {
    // Clear any previous runs, for idempotence
    log.info("Clearing donor.genes...");
    repository.unsetDonorGenes();
    log.info("Finished clearing donor.genes");

    // Order is important:
    summarizeDonorGenes();
    summarizeDonorRepositories();
    summarizeDonorStudies();
    summarizeDonorCompleteness();
    summarizeDonorLibraryStrategies();
    summarizeDonorAgeGroups();
  }

  private void summarizeDonorGenes() {
    val observations = repository.getDonorGeneObservationTypes();
    val watch = Stopwatch.createStarted();

    // Accumulation state
    int count = 0;
    String currentDonorId = null;
    val table = TreeBasedTable.<String, String, Integer> create();

    for (val observation : observations) {
      try {
        val nextDonorId = observation.get(OBSERVATION_DONOR_ID).asText();
        val type = observation.get(OBSERVATION_TYPE).asText();
        val consequences = observation.path(OBSERVATION_CONSEQUENCES);

        val first = currentDonorId == null;
        if (first) {
          currentDonorId = nextDonorId;
        }

        val commit = !currentDonorId.equals(nextDonorId);
        if (commit) {
          // Commit
          val result = createDonorGeneResult(currentDonorId, table);
          summarizeDonorGene(result);

          if (++count % COUNTER_THRESHOLD == 0) {
            log.info("Summarized {} donor-gene(s) ({} docs/s)",
                formatCount(count), formatRate(rate(COUNTER_THRESHOLD, watch)));
          }

          // Advance and clean-up
          currentDonorId = nextDonorId;
          table.clear();
        }

        // Find unique geneIds for this observation
        val uniqueGeneIds = getUniqueGeneIds(consequences, true);

        // Accumulate
        for (val geneId : uniqueGeneIds) {
          val typeCount = firstNonNull(table.get(geneId, type), 0);
          table.put(geneId, type, typeCount + 1);
        }
      } catch (Exception e) {
        log.error("Error processing observation: " + observation, e);
        throw new RuntimeException(e);
      }
    }

    val commit = !table.isEmpty();
    if (commit) {
      // Commit the straggler, if any
      val result = createDonorGeneResult(currentDonorId, table);
      summarizeDonorGene(result);
    }
  }

  private void summarizeDonorGene(JsonNode result) {
    val donorId = result.get("donorId").textValue();
    val genes = result.get("genes");

    val uniqueGeneIds = Sets.<String> newTreeSet();
    for (val gene : genes) {
      val geneId = nullToEmpty(gene.path("geneId").textValue());
      uniqueGeneIds.add(geneId);

      ObjectNode donorGeneSummary = createDonorGeneSummary(geneId);
      for (val typeCount : gene.get("typeCounts")) {
        // Shorthands
        String type = typeCount.get("type").textValue();
        int count = typeCount.get("typeCount").intValue();

        // DCC-1401: Only ssm for now
        if (type.equals(SSM_TYPE.getId())) {
          setTypeMetric(donorGeneSummary.with(DONOR_GENE_SUMMARY), SSM_TYPE, count);
        }
      }

      repository.pushDonorGeneSummary(donorId, donorGeneSummary);
    }

    int affectedGeneCount = uniqueGeneIds.size();
    repository.setDonorAffectedGeneCount(donorId, affectedGeneCount);
  }

  private void summarizeDonorRepositories() {
    val results = repository.getDonorRepositories();
    for (val result : results) {
      String donorId = result.get("donorId").textValue();
      ArrayNode repositories = (ArrayNode) result.get("repositories");

      try {
        repository.setDonorRepositories(donorId, repositories);
      } catch (Throwable t) {
        log.error("Error setting donor repository with donorId '{}' and repositories '{}'", donorId,
            repositories);
        throw new RuntimeException(t);
      }
    }
  }

  private void summarizeDonorStudies() {
    val results = repository.getDonorStudies();
    for (val result : results) {
      String donorId = result.get("donorId").textValue();
      ArrayNode studies = (ArrayNode) result.get("study");

      try {
        repository.setDonorStudies(donorId, studies);
      } catch (Throwable t) {
        log.error("Error setting donor study with donorId '{}' and studies '{}'", donorId,
            studies);
        throw new RuntimeException(t);
      }
    }
  }

  private void summarizeDonorCompleteness() {
    val results = repository.getDonorAvailableDataTypes();
    for (val entry : results.entrySet()) {
      val donorId = entry.getKey();
      val donorAvailableDataTypes = entry.getValue();

      val complete = !donorAvailableDataTypes.isEmpty();

      try {
        repository.setDonorCompleteness(donorId, complete);
      } catch (Throwable t) {
        log.error("Error setting donor completeness with donorId '{}' and completeness '{}'", donorId, complete);
        throw new RuntimeException(t);
      }
    }
  }

  private void summarizeDonorLibraryStrategies() {
    val results = repository.getDonorLibraryStrategyCounts();
    for (val result : results) {
      String donorId = result.get("donorId").textValue();
      ArrayNode libraryStrategies = (ArrayNode) result.get("libraryStrategyCounts");

      try {
        repository.setDonorExperimentalAnalysis(donorId, libraryStrategies);
      } catch (Throwable t) {
        log.error("Error setting donor experimental analysis with donorId '{}' and libraryStrategies '{}'", donorId,
            libraryStrategies);
        throw new RuntimeException(t);
      }
    }
  }

  private void summarizeDonorAgeGroups() {
    val results = repository.getDonorAges();
    for (val entry : results.entrySet()) {
      String donorId = entry.getKey();
      String age = entry.getValue();
      String ageGroup = getAgeGroup(age);

      repository.setDonorAgeGroup(donorId, ageGroup);
    }
  }

  private ObjectNode createDonorGeneResult(String donorId, Table<String, String, Integer> table) {
    val result = createObject();
    result.put("donorId", donorId);

    val genes = result.withArray("genes");
    for (val row : table.rowMap().entrySet()) {
      val geneId = row.getKey();
      val geneTypeCounts = row.getValue();

      val gene = createObject();
      gene.put("geneId", geneId);
      val typeCounts = gene.withArray("typeCounts");
      for (val entry : geneTypeCounts.entrySet()) {
        val type = entry.getKey();
        val count = entry.getValue();

        val typeCount = createObject();
        typeCount.put("type", type);
        typeCount.put("typeCount", count);

        typeCounts.add(typeCount);
      }

      genes.add(gene);
    }

    return result;
  }

  private ObjectNode createDonorGeneSummary(String geneId) {
    ObjectNode donorGeneSummary = createSummary();
    donorGeneSummary.put(DONOR_GENE_GENE_ID, geneId);

    // Create defaults
    // DCC-1401: Only ssm for now
    ObjectNode summary = donorGeneSummary.with(DONOR_GENE_SUMMARY);
    setTypeMetric(summary, SSM_TYPE, 0);

    return donorGeneSummary;
  }

}
