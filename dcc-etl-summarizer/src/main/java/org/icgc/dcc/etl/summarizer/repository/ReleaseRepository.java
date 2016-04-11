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
package org.icgc.dcc.etl.summarizer.repository;

import static java.lang.String.format;
import static org.icgc.dcc.common.core.json.Jackson.asArrayNode;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_AGE_AT_DIAGNOSIS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_SEQUENCE_DATA;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_STUDY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_AFFECTED_GENE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_STATE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_STUDIES;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SETS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SETS_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SET_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCE_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_PRIMARY_SITE;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY_STATE;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_LIBRARY_STRATEGY;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SPECIMEN_COUNT;
import static org.icgc.dcc.etl.summarizer.util.JsonNodes.extractStudies;
import static org.icgc.dcc.etl.summarizer.util.JsonNodes.mapLongValues;
import static org.icgc.dcc.etl.summarizer.util.JsonNodes.mapMultipleTextValues;
import static org.icgc.dcc.etl.summarizer.util.JsonNodes.mapTextValues;
import static org.icgc.dcc.etl.summarizer.util.JsonNodes.textValues;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.bson.types.ObjectId;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.util.Separators;
import org.icgc.dcc.etl.summarizer.util.LongBatchQueryModifier;
import org.jongo.MongoCollection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@RequiredArgsConstructor
public class ReleaseRepository {

  /**
   * Constants.
   */

  @NonNull
  private final MongoCollection genes;
  @NonNull
  private final MongoCollection observations;
  @NonNull
  private final MongoCollection donors;
  @NonNull
  private final MongoCollection projects;
  @NonNull
  private final MongoCollection mutations;
  @NonNull
  private final MongoCollection releases;
  @NonNull
  private final MongoCollection geneSets;

  public Map<String, String> getDonorProjectIds() {
    // Create donorId -> projectId mapping
    return mapTextValues(DONOR_ID, DONOR_PROJECT_ID, donors
        .find()
        .projection("{ _id: 0, " + DONOR_ID + ": 1, " + DONOR_PROJECT_ID + ": 1 }")
        .as(JsonNode.class));
  }

  public Map<String, String> getDonorAges() {
    // Create donorId -> age mapping
    return mapTextValues(DONOR_ID, DONOR_AGE_AT_DIAGNOSIS, donors
        .find()
        .projection("{ _id: 0, " + DONOR_ID + ": 1, " + DONOR_AGE_AT_DIAGNOSIS + ": 1 }")
        .as(JsonNode.class));
  }

  public Map<String, List<String>> getDonorAvailableDataTypes() {
    val availableDataTypes = DONOR_SUMMARY + "." + AVAILABLE_DATA_TYPES;
    val field = "dataTypeCount";

    // Create donorId -> data type count mapping
    return mapMultipleTextValues(
        DONOR_ID, field,
        donors
            .aggregate(
                "{ $project: { _id: 0, " + DONOR_ID + ": 1, " + field + ": '$" + availableDataTypes
                    + "' } }")
            .as(JsonNode.class));
  }

  public Iterable<JsonNode> getDonorGeneObservationTypes() {
    return observations
        .find()
        .projection("{ "
            + "_id: 0, "
            + OBSERVATION_DONOR_ID + ": 1, "
            + OBSERVATION_TYPE + ": 1, '"
            + OBSERVATION_CONSEQUENCES + "." + OBSERVATION_CONSEQUENCES_GENE_ID + "': 1 }")
        .sort("{ " + OBSERVATION_DONOR_ID + ": 1 }")
        .as(JsonNode.class);
  }

  public List<JsonNode> getDonorLibraryStrategyCounts() {
    String samples = DONOR_SPECIMEN + "." + DONOR_SAMPLE;
    String sampleId = DONOR_SAMPLE_ID;
    String libraryStategy = DONOR_SAMPLE_SEQUENCE_DATA + "." + SEQUENCE_DATA_LIBRARY_STRATEGY;

    return donors
        .aggregate(
            "{ $project: { _id: 0, donorId: '$" + DONOR_ID + "', specimen: '$" + samples + "' } }")
        .and(
            "{ $unwind: '$specimen' }")
        .and(
            "{ $unwind: '$specimen' }")
        .and(
            "{ $project : { '_id':0, 'donorId': '$donorId', 'sampleId': '$specimen." + sampleId + "', " +
                "'libraryStrategy': '$specimen." + libraryStategy + "' } }")
        .and(
            "{ $unwind: '$libraryStrategy' }")
        .and(
            "{ $group: { _id: { donorId: '$donorId', sampleId: '$sampleId', libraryStrategy: '$libraryStrategy' } } }")
        .and(
            "{ $group: { _id: { donorId: '$_id.donorId', libraryStrategy: '$_id.libraryStrategy' }, libraryStrategyCount: { $sum: 1 } } }")
        .and(
            "{ $group: { _id: { donorId: '$_id.donorId' }, libraryStrategyCounts: { $push : { libraryStrategy: '$_id.libraryStrategy', libraryStrategyCount: '$libraryStrategyCount' } } } }")
        .and(
            "{ $project: { _id: 0, donorId: '$_id.donorId', libraryStrategyCounts: 1 } }")
        .as(JsonNode.class);
  }

  public List<JsonNode> getDonorRepositories() {
    String repository =
        DONOR_SPECIMEN + "." + DONOR_SAMPLE + "." + DONOR_SAMPLE_SEQUENCE_DATA + "." + SEQUENCE_DATA_REPOSITORY;

    return donors
        .aggregate(
            "{ $project: { _id: 0, donorId: '$" + DONOR_ID + "', repository: '$" + repository + "' } }")
        .and(
            "{ $unwind: '$repository' }")
        .and(
            "{ $unwind: '$repository' }")
        .and(
            "{ $unwind: '$repository' }")
        .and(
            "{ $group: { _id: { donorId: '$donorId' }, repositories: { $addToSet : '$repository' } } }")
        .and(
            "{ $project: { _id: 0, donorId: '$_id.donorId', repositories: 1 } }")
        .as(JsonNode.class);
  }

  public void setDonorRepositories(String donorId, JsonNode repositories) {
    String field = DONOR_SUMMARY + "." + DONOR_SUMMARY_REPOSITORY;
    donors
        .update("{ " + DONOR_ID + ": # }", donorId)
        .with("{ $set: { " + field + ": # } }", textValues(repositories));
  }

  public List<JsonNode> getDonorStudies() {
    String study =
        DONOR_SPECIMEN + "." + DONOR_SAMPLE + "." + DONOR_SAMPLE_STUDY;

    return donors
        .aggregate(
            "{ $project: { _id: 0, donorId: '$" + DONOR_ID + "', study: '$" + study + "' } }")
        .and(
            "{ $unwind: '$study' }")
        .and(
            "{ $group: { _id: { donorId: '$donorId' }, study: { $addToSet : '$study' } } }")
        .and(
            "{ $project: { _id: 0, donorId: '$_id.donorId', study: 1 } }")
        .as(JsonNode.class);
  }

  public void setDonorStudies(String donorId, JsonNode studies) {
    String field = DONOR_SUMMARY + "." + DONOR_SUMMARY_STUDIES;
    donors
        .update("{ " + DONOR_ID + ": # }", donorId)
        .with("{ $set: { " + field + ": # } }", extractStudies(studies));
  }

  public void setDonorState(String donorId, String state) {
    String field = DONOR_SUMMARY + "." + DONOR_SUMMARY_STATE;
    donors
        .update("{ " + DONOR_ID + ": # }", donorId)
        .with("{ $set: { " + field + ": # } }", state);
  }

  public void setDonorExperimentalAnalysis(String donorId, JsonNode analysisSampleCounts) {
    String f1 = DONOR_SUMMARY + "." + DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS;
    String f2 = DONOR_SUMMARY + "." + DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS;
    donors
        .update("{ " + DONOR_ID + ": # }", donorId)
        .with("{ $set: { " + f1 + ": #, " + f2 + ": # } }",
            textValues("libraryStrategy", analysisSampleCounts),
            mapLongValues("libraryStrategy", "libraryStrategyCount", analysisSampleCounts));
  }

  public void setDonorAgeGroup(String donorId, String ageRange) {
    donors
        .update("{ " + DONOR_ID + ": # }", donorId)
        .with("{ $set: { " + DONOR_SUMMARY + "." + DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP + ": # } }",
            ageRange);
  }

  public void setDonorAffectedGeneCount(String donorId, int affectedGeneCount) {
    donors
        .update("{ " + DONOR_ID + ": # }", donorId)
        .with("{ $set: { " + DONOR_SUMMARY + "." + DONOR_SUMMARY_AFFECTED_GENE_COUNT + ": # } }",
            affectedGeneCount);
  }

  public void setDonorGeneSummary(String donorId, ObjectNode donoGeneSummary) {
    val genes = donoGeneSummary.get(DONOR_GENES);
    donors
        .update("{ " + DONOR_ID + ": # }", donorId)
        .with("{ $set: { " + DONOR_GENES + ": # } }", jsonNodes(asArrayNode(genes)));
  }

  public void unsetDonorGenes() {
    donors
        .update("{}")
        .multi()
        .with("{ $unset: { " + DONOR_GENES + ": # } }", "");
  }

  public int setGeneSummary(String geneId, ObjectNode geneSummary) {
    return genes
        .update("{ " + GENE_ID + ": # }", geneId)
        .with("{ $set: # }", geneSummary)
        .getN();
  }

  public Iterable<ObjectNode> getGeneSets() {
    return geneSets
        .find("{}")
        .with(new LongBatchQueryModifier())
        .as(ObjectNode.class);
  }

  public void saveGeneSet(ObjectNode geneSet) {
    geneSets.save(geneSet);
  }

  public List<ObjectNode> getGeneSetIdTypeCounts() {
    return genes
        .aggregate(format("{ '$project' : { _id: 0, sets: '$%s' } }", GENE_SETS))
        .and(format("{ $unwind: '$%s' }", GENE_SETS))
        .and(format("{ '$project' : { idType: { $concat:['$%s', '%s', '$%s'] } } }",
            getFieldName(GENE_SETS, GENE_SET_ID), Separators.DASH, getFieldName(GENE_SETS, GENE_SETS_TYPE)))
        .and("{ $group: { _id: '$idType', count: { $sum: 1 } } }")
        .and("{ '$project' : { _id: 0, idType: '$_id', count: 1 } }")
        .as(ObjectNode.class);
  }

  public Iterable<ObjectNode> getObservationConsequenceTypes() {
    return observations
        .find()
        .projection("{ '" + OBSERVATION_CONSEQUENCES + "." + OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE + "':1 }")
        .as(ObjectNode.class);
  }

  public int setObservationSummary(ObjectId observationId, Set<String> consequenceTypes) {
    return observations
        .update("{ " + OBSERVATION_ID + ": # }", observationId)
        .with("{ $set: { " + OBSERVATION_CONSEQUENCE_TYPES + ": # } }", consequenceTypes)
        .getN();
  }

  public List<String> getProjectIds() {
    return textValues(PROJECT_ID,
        projects
            .find()
            .projection("{ " + PROJECT_ID + ": 1 }")
            .as(JsonNode.class));
  }

  public List<JsonNode> getProjectObservationTypes() {
    // @formatter:off
    // Use Donor collection as the basis for the pipeline (previously summarized by DonorSummarizer)
    return donors
        // Retain only the required fields to reduce memory footprint and rename to internal field names: 
        //  {projectId: "p1", types: [{...},{...}]}               
        .aggregate("{ '$project' : { _id: 0, projectId: '$" + DONOR_PROJECT_ID +"', types: '$" + DONOR_SUMMARY + "." + AVAILABLE_DATA_TYPES + "' } }")
        
        // Create repeating (projectId, type) combinations since they are not unique across donors
        //  {projectId: "p1", types: "ssm"}           
        //  {projectId: "p1", types: "ssm"}           
        .and("{ $unwind: '$types' }")

        // Generate unique project-type combinations          
        .and("{ $group: { _id: { projectId: '$projectId', type: '$types'} } }")
        
        // Pivot
        //  {projectId: "p1", types: ["ssm"]}
        .and("{ $group: { _id: '$_id.projectId', types: { $addToSet : '$_id.type' } } }")
        
        // Remove unused fields
        .and("{ $project: { _id: 0, projectId: '$_id', types: 1 } } ")
        
        // Return list of previous objects
        .as(JsonNode.class);
    // @formatter:on
  }

  public List<JsonNode> getProjectObservationTypeCount(FeatureType type) {
    String typeFieldName = DONOR_SUMMARY + "." + type.getSummaryFieldName();
    String expression = "'$" + typeFieldName + "'";
    String condition = type.isCountSummary() ? "{ $gt: [" + expression + ", 0] }" : expression;

    // @formatter:off
    // Use Donor collection as the basis for the pipeline
    return donors
        // Retain only the required fields to reduce memory footprint and rename to internal field names: 
        //  {projectId: "p1", typeCount: 0}           
        //  {projectId: "p1", typeCount: 1}           
        //  {projectId: "p1", typeCount: 1}           
        .aggregate("{ '$project' : { _id: 0, projectId: '$" + DONOR_PROJECT_ID + "', typeCount: { $cond: [ " + condition + ", 1, 0 ] } } }")
        
        // Sum within group:
        //  {projectId: "p1", typeCount: 2}      
        .and("{ $group: { _id: '$projectId', typeCount: { $sum: '$typeCount' } } }")
        
         // Remove unused fields
        .and("{ '$project' : { _id: 0, projectId: '$_id', typeCount: 1 } }")
  
        // Return list of previous objects
        .as(JsonNode.class);
    // @formatter:on
  }

  public List<JsonNode> getProjectDonorCounts() {
    // @formatter:off
    // Use Donor collection as the basis for the pipeline    
    return donors
        // Retain only the required fields to reduce memory footprint and rename to internal field names: 
        //  {projectId: "p1"}        
        //  {projectId: "p1"}        
        .aggregate("{ $project: { projectId: '$" + DONOR_PROJECT_ID + "' } }")
        
        //  {projectId: "p1", count: 2}
        .and("{ $group: { _id: '$projectId', donorCount: { $sum: 1 } } } }")
        
         // Remove unused fields
        .and("{ $project: { _id: 0, projectId: '$_id', donorCount: 1 } } ")
        
         // Return list of previous objects
        .as(JsonNode.class);
    // @formatter:on
  }

  public List<JsonNode> getProjectLiveDonorCounts() {
    // @formatter:off
    // Use Donor collection as the basis for the pipeline    
    return donors
        // Retain only the required fields to reduce memory footprint and rename to internal field names: 
        //  {projectId: "p1"}        
        //  {projectId: "p1"}        
        .aggregate("{ $match: { '" + DONOR_SUMMARY + "." + DONOR_SUMMARY_STATE + "': 'live' } }")
        
        .and("{ $project: { projectId: '$" + DONOR_PROJECT_ID + "' } }")
        
        //  {projectId: "p1", count: 2}
        .and("{ $group: { _id: '$projectId', donorCount: { $sum: 1 } } } }")
        
         // Remove unused fields
        .and("{ $project: { _id: 0, projectId: '$_id', donorCount: 1 } } ")
        
         // Return list of previous objects
        .as(JsonNode.class);
    // @formatter:on
  }

  public List<JsonNode> getProjectSampleSpecimenCounts() {
    // @formatter:off
    // Use Donor collection as the basis for the pipeline
    return donors
        // Retain only the required fields to reduce memory footprint and rename to internal field names: 
        //  {donorId: "d1", projectId: "p1", specimen: [{...}, {...}]}        
        .aggregate("{ $project: { donorId: '$" + DONOR_ID + "', projectId: '$" + DONOR_PROJECT_ID + "', specimen: '$" + DONOR_SPECIMEN + "' } }")
        
        // Create repeated (donorId, projectId) combinations with unique specimen(s): 
        //  {donorId: "d1", projectId: "p1", specimen: {...}}
        //  {donorId: "d1", projectId: "p1", specimen: {...}}        
        .and("{ $unwind: '$specimen' }")
        
        // Retain only the required fields to reduce memory footprint and rename to internal field names: 
        //  {donorId: "d1", projectId: "p1", specimenId: "sp1", sample: [{...}, {...}]}
        //  {donorId: "d1", projectId: "p1", specimenId: "sp2", sample: [{...}, {...}]}              
        .and("{ $project: { _id: 0, projectId: 1, donorId: 1, specimenId: '$specimen." + DONOR_SPECIMEN_ID + "', sample: '$specimen." + DONOR_SAMPLE + "' } }")
        
        // Create repeated (donorId, projectId) combinations with unique samples(s): 
        //  {donorId: "d1", projectId: "p1", specimenId: "sp1", sample: {...}}
        //  {donorId: "d1", projectId: "p1", specimenId: "sp1", sample: {...}}
        //  {donorId: "d1", projectId: "p1", specimenId: "sp2", sample: {...}}
        //  {donorId: "d1", projectId: "p1", specimenId: "sp2", sample: {...}}
        .and("{ $unwind: '$sample' }")
        
        // Retain only the required fields to reduce memory footprint and rename / project to internal field names: 
        //  {donorId: "d1", projectId: "p1", specimenId: "sp1", sampleId: ["sa11", "sa12"]}
        //  {donorId: "d1", projectId: "p1", specimenId: "sp2", sampleId: ["sa21", "sa22"]}              
        .and("{ $project: { _id: 0, projectId: 1, donorId: 1, specimenId: 1, sampleId: '$sample." + DONOR_SAMPLE_ID + "' } }")
        
        // Group by (projectId, donorId, specimenId) and count samples:
        //  {projectId: "p1", donorId: "d1", specimenId: "sp1", sampleCount: 2} 
        .and("{ $group: { _id: { projectId: '$projectId', donorId: '$donorId', specimenId: '$specimenId' }, sampleCount: { $sum: 1 } } }")
        
        // Group by (projectId) and count speciments and samples:
        //  {projectId: "p1", specimenCount: 1, sampleCount: 2}         
        .and("{ $group: { _id: { projectId: '$_id.projectId' }, specimenCount: { $sum: 1 }, sampleCount: { $sum: '$sampleCount' } } }")
        
        // Remove unused fields
        .and("{ $project: { _id: 0 , projectId: '$_id.projectId', specimenCount: 1, sampleCount: 1 } }")
        
        // Return list of previous objects
        .as(JsonNode.class);
    // @formatter:on
  }

  public List<JsonNode> getProjectRepositories() {
    String repository = DONOR_SUMMARY + "." + DONOR_SUMMARY_REPOSITORY;
    return donors
        .aggregate(
            "{ '$project' : { _id: 0, projectId: '$" + DONOR_PROJECT_ID + "', repository: '$" + repository + "' } }")
        .and(
            "{ $unwind: '$repository' }")
        .and(
            "{ $group: { _id: '$projectId', repositories: { $addToSet : '$repository' } } }")
        .and(
            "{ $project: { _id: 0, projectId: '$_id', repositories: 1 } }")
        .as(JsonNode.class);
  }

  public List<JsonNode> getProjectLibraryStrategySampleCounts() {
    String donorSampleCounts = DONOR_SUMMARY + "." + DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS;
    return donors
        .aggregate(
            "{ $project: { _id: 0,  projectId: '$" + DONOR_PROJECT_ID + "', donorSampleCounts: '$" + donorSampleCounts
                + "' } }")
        .and(
            "{ $group: { _id: { projectId: '$projectId' }, sampleCounts: { $push : '$donorSampleCounts' } } }")
        .and(
            "{ $project: { _id: 0, projectId: '$_id.projectId', sampleCounts: 1 } }")
        .as(JsonNode.class);
  }

  public void setProjectSummary(String projectId, ObjectNode projectSummary) {
    projects
        .update("{ " + PROJECT_ID + ": # }", projectId)
        .with("{ $set: { " + PROJECT_SUMMARY + ": # } }", projectSummary);
  }

  public long getReleaseProjectCount() {
    return projects.count();
  }

  public long getReleaseLiveProjectCount() {
    return projects.count("{ '" + PROJECT_SUMMARY + "." + PROJECT_SUMMARY_STATE + "': 'live' }");
  }

  public long getReleaseUniqueAffectedGeneCount() {
    return genes.count("{ project: { $exists : 1 } }");
  }

  public long getReleaseUniqueSsmCount() {
    return mutations.count();
  }

  public int getReleaseUniquePrimarySiteCount() {
    return projects.distinct(PROJECT_PRIMARY_SITE).as(String.class).size();
  }

  public long getReleaseUniqueLivePrimarySiteCount() {
    return projects.distinct(PROJECT_PRIMARY_SITE)
        .query("{ '" + PROJECT_SUMMARY + "." + PROJECT_SUMMARY_STATE + "': 'live' }")
        .as(String.class)
        .size();
  }

  public long getReleaseDonorCount() {
    return donors.count();
  }

  public long getReleaseLiveDonorCount() {
    return donors.count("{ '" + DONOR_SUMMARY + "." + DONOR_SUMMARY_STATE + "': 'live' }");
  }

  public void saveRelease(ObjectNode release) {
    releases.save(release);
  }

  public long getReleaseSpecimenCount() {
    String specimenCount = PROJECT_SUMMARY + "." + TOTAL_SPECIMEN_COUNT;

    return projects
        .aggregate(
            "{ $group: { _id: '', specimenCount: { $sum: '$" + specimenCount + "' } } } ")
        .and(
            "{ $project: { _id: 0, specimenCount: '$specimenCount' } }")
        .as(JsonNode.class)
        .get(0).get("specimenCount").asLong();
  }

  public long getReleaseSampleCount() {
    String sampleCount = PROJECT_SUMMARY + "." + TOTAL_SAMPLE_COUNT;

    return projects
        .aggregate(
            "{ $group: { _id: '', sampleCount: { $sum: '$" + sampleCount + "' } } }")
        .and(
            "{ $project: { _id: 0, sampleCount: '$sampleCount' } }")
        .as(JsonNode.class)
        .get(0).get("sampleCount").asLong();
  }

  private static List<JsonNode> jsonNodes(ArrayNode array) {
    val nodesList = ImmutableList.<JsonNode> builder();
    for (val element : array) {
      nodesList.add(element);
    }

    return nodesList.build();
  }

  private static String getFieldName(String parent, String child) {
    return parent + Separators.DOT + child;
  }

}
