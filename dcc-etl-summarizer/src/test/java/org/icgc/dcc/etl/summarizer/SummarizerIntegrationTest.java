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
package org.icgc.dcc.etl.summarizer;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.Component.SUMMARIZER;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.CNGV_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.CNSM_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.EXP_ARRAY_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.EXP_SEQ_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.JCN_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.METH_ARRAY_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.METH_SEQ_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.MIRNA_SEQ_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.PEXP_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SGV_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.STGV_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.STSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.AFFECTED_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_EXPERIMENTAL_ANALYSIS_PERFORMED;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_AGE_AT_DIAGNOSIS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_SEQUENCE_DATA;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_AFFECTED_GENE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_STUDIES;
import static org.icgc.dcc.common.core.model.FieldNames.EXPERIMENTAL_ANALYSIS_PERFORMED_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.EXPERIMENTAL_ANALYSIS_PERFORMED_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONORS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECTS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.MONGO_INTERNAL_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCE_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY_STATE;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_DATE;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_LIVE_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_LIVE_PROJECT_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_MUTATED_GENE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_NUMBER;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_PRIMARY_SITE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_PROJECT_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_SPECIMEN_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_SSM_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_LIBRARY_STRATEGY;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SPECIMEN_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.getTestedTypeCountFieldName;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DONOR_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.RELEASE_COLLECTION;
import static org.icgc.dcc.common.test.Tests.TEST_PATCH_NUMBER;
import static org.icgc.dcc.common.test.Tests.TEST_RUN_NUMBER;
import static org.icgc.dcc.common.test.Tests.getTestJobId;
import static org.icgc.dcc.common.test.Tests.getTestReleasePrefix;
import static org.icgc.dcc.common.test.fest.JsonNodeAssert.assertThat;
import static org.icgc.dcc.common.test.json.JsonNodes.$;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.bson.types.ObjectId;
import org.icgc.dcc.common.core.Component;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.common.test.Tests;
import org.icgc.dcc.common.test.mongodb.EmbeddedMongo;
import org.icgc.dcc.common.test.mongodb.MongoImporter;
import org.icgc.dcc.etl.summarizer.service.SummarizerService;
import org.joda.time.DateTimeUtils;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.google.common.collect.ImmutableMap;
import com.mongodb.DB;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SummarizerIntegrationTest {

  static {
    // Enable MongoDB logging in general
    System.setProperty("DEBUG.MONGO", "true");

    // Enable DB operation tracing
    System.setProperty("DB.TRACE", "true");
  }

  /**
   * Test data.
   */
  private static final Component TESTED_COMPONENT = SUMMARIZER;
  private static final String TEST_JOB_ID = getTestJobId(TESTED_COMPONENT);
  private static final String TEST_RELEASE_NAME = getTestReleasePrefix(TESTED_COMPONENT) + TEST_PATCH_NUMBER;

  //@formatter:off
  private static final Map<String, JsonNode> TEST_DATA = ImmutableMap.<String, JsonNode> builder()
      .put("donor1", $(
          "{" +
              "_id:'1'," +
              DONOR_ID + ":'" + "donor1" + "'," +
              PROJECT_ID + ":'project1'," +
              DONOR_AGE_AT_DIAGNOSIS + ":5," +
              
              // Donor-observation summaries
              DONOR_SUMMARY + ":{" +
                DONOR_SUMMARY_AFFECTED_GENE_COUNT + ":4," +
                DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP + ":'1 - 9'," +
                AVAILABLE_DATA_TYPES + ":['ssm','cnsm']," +
                SSM_TYPE.getSummaryFieldName()  + ":1," +
                
                CNSM_TYPE.getSummaryFieldName() + ":true," +
                CNGV_TYPE.getSummaryFieldName() + ":false," +
                SGV_TYPE.getSummaryFieldName()  + ":false," +
                STGV_TYPE.getSummaryFieldName() + ":false," +
                STSM_TYPE.getSummaryFieldName() + ":false," +
                EXP_ARRAY_TYPE.getSummaryFieldName()   + ":false," +
                EXP_SEQ_TYPE.getSummaryFieldName()   + ":false," +
                METH_ARRAY_TYPE.getSummaryFieldName()   + ":false," +
                METH_SEQ_TYPE.getSummaryFieldName()   + ":false," +
                MIRNA_SEQ_TYPE.getSummaryFieldName()   + ":false," +
                JCN_TYPE.getSummaryFieldName()   + ":false," +
                PEXP_TYPE.getSummaryFieldName()  + ":false," +
                
                DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS + ":[" +        
                  "'VALIDATION','WXS'" +
                "]," +
                DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS + ":{" +        
                  "VALIDATION:1," +
                  "WXS:1" +
                "}," +
                DONOR_SUMMARY_REPOSITORY + ":[" +        
                  "'EGA','CGHub'" +
                "]," +
                DONOR_SUMMARY_STUDIES + ":[" +        
                "'study1'" +
                "]" +
              "}," +
              
              // Donor-gene summaries
              DONOR_GENES + ":[" +
                "{" +
                DONOR_GENE_SUMMARY + ":{" +
                  SSM_TYPE.getSummaryFieldName()  + ":0" +
                "}," +        
                  DONOR_GENE_GENE_ID + ":''" +
                "}," +        
                "{" +
                  DONOR_GENE_SUMMARY + ":{" +
                    SSM_TYPE.getSummaryFieldName()  + ":2" +
                  "}," +        
                  DONOR_GENE_GENE_ID + ":'gene1'" +
                "}," +
                "{" +
                  DONOR_GENE_SUMMARY + ":{" +          
                  SSM_TYPE.getSummaryFieldName()  + ":0" +
                  "}," +               
                  DONOR_GENE_GENE_ID + ":'gene3'" +
                "}," +          
                "{" +
                  DONOR_GENE_SUMMARY + ":{" +          
                    SSM_TYPE.getSummaryFieldName()  + ":2" +
                  "}," +               
                  DONOR_GENE_GENE_ID + ":'gene2'" +
                "}" +               
              "]," +
              
              DONOR_SPECIMEN + ":[{" +
                DONOR_SPECIMEN_ID + ":'specimen1'," +
                DONOR_SAMPLE + ":[{" +
                  DONOR_SAMPLE_ID + ":'sample1'," +
                  DONOR_SAMPLE_SEQUENCE_DATA + ":[{" +
                    SEQUENCE_DATA_REPOSITORY + ":'CGHub'," +
                    SEQUENCE_DATA_LIBRARY_STRATEGY + ":'WXS'" +
                  "},{" +
                    SEQUENCE_DATA_REPOSITORY + ":'EGA'," +
                    SEQUENCE_DATA_LIBRARY_STRATEGY + ":'VALIDATION'" +
                  "}]" +
                "}]" +
              "}]" +
            "}"
          ))
          
      .put("donor2", $(
          "{" +
              "_id:'2'," +
              DONOR_ID + ":'" + "donor2" + "'," +
              PROJECT_ID + ":'project1'," +
              DONOR_AGE_AT_DIAGNOSIS + ":5," +
              
              // Donor-observation summaries
              DONOR_SUMMARY + ":{" +
                DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP + ":'1 - 9'," +
                AVAILABLE_DATA_TYPES + ":['ssm','cnsm']," +
                SSM_TYPE.getSummaryFieldName()  + ":1," +
                
                CNSM_TYPE.getSummaryFieldName() + ":true," +
                CNGV_TYPE.getSummaryFieldName() + ":false," +
                SGV_TYPE.getSummaryFieldName()  + ":false," +
                STGV_TYPE.getSummaryFieldName() + ":false," +
                STSM_TYPE.getSummaryFieldName() + ":false," +
                EXP_ARRAY_TYPE.getSummaryFieldName()   + ":false," +
                EXP_SEQ_TYPE.getSummaryFieldName()   + ":false," +
                METH_ARRAY_TYPE.getSummaryFieldName()   + ":false," +
                METH_SEQ_TYPE.getSummaryFieldName()   + ":false," +
                MIRNA_SEQ_TYPE.getSummaryFieldName()   + ":false," +
                JCN_TYPE.getSummaryFieldName()   + ":false," +
                PEXP_TYPE.getSummaryFieldName()  + ":false," +
                
                DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS + ":[" +        
                  "'VALIDATION','WXS'" +
                "]," +
                DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS + ":{" +        
                  "VALIDATION:1," +
                  "WXS:1" +
                "}," +
                DONOR_SUMMARY_REPOSITORY + ":[" +        
                  "'CGHub'" +
                "]," +
                DONOR_SUMMARY_STUDIES + ":[]" +
              "}," +
              
              DONOR_SPECIMEN + ":[{" +
                DONOR_SPECIMEN_ID + ":'specimen1'," +
                DONOR_SAMPLE + ":[{" +
                  DONOR_SAMPLE_ID + ":'sample1'," +
                  DONOR_SAMPLE_SEQUENCE_DATA + ":[{" +
                    SEQUENCE_DATA_REPOSITORY + ":'CGHub'," +
                    SEQUENCE_DATA_LIBRARY_STRATEGY + ":'WXS'" +
                  "},{" +
                    SEQUENCE_DATA_REPOSITORY + ":'CGHub'," +
                    SEQUENCE_DATA_LIBRARY_STRATEGY + ":'VALIDATION'" +
                  "}]" +
                "}]" +
              "}]" +
            "}"
          ))
          
      .put("donor3", $(
          "{" +
              "_id:'3'," +
              DONOR_ID + ":'" + "donor3" + "'," +
              PROJECT_ID + ":'project1'," +
              DONOR_AGE_AT_DIAGNOSIS + ":77," +
              
              // Donor-observation summaries
              DONOR_SUMMARY + ":{" +
                DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP + ":'70 - 79'," +
                AVAILABLE_DATA_TYPES + ":[]," +
                SSM_TYPE.getSummaryFieldName()  + ":0," +
                
                CNSM_TYPE.getSummaryFieldName() + ":false," +
                CNGV_TYPE.getSummaryFieldName() + ":false," +
                SGV_TYPE.getSummaryFieldName()  + ":false," +
                STGV_TYPE.getSummaryFieldName() + ":false," +
                STSM_TYPE.getSummaryFieldName() + ":false," +
                EXP_ARRAY_TYPE.getSummaryFieldName()   + ":false," +
                EXP_SEQ_TYPE.getSummaryFieldName()   + ":false," +
                METH_ARRAY_TYPE.getSummaryFieldName()   + ":false," +
                METH_SEQ_TYPE.getSummaryFieldName()   + ":false," +
                MIRNA_SEQ_TYPE.getSummaryFieldName()   + ":false," +
                JCN_TYPE.getSummaryFieldName()   + ":false," +
                PEXP_TYPE.getSummaryFieldName()  + ":false" +
              "}," +
              
              DONOR_SPECIMEN + ":[]" +
            "}"
          ))          
      .build();
  
  //@formatter:on

  /**
   * Test configuration.
   */
  private String mongoUri;

  /**
   * Class under test.
   */
  private SummarizerService service;

  /**
   * Test environment.
   */
  @Rule
  public final EmbeddedMongo embeddedMongo = new EmbeddedMongo();
  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Before
  public void setUp() {
    importData();
    configure();
  }

  @Test
  public void testSummarizer() throws IOException {
    service.summarize(TEST_JOB_ID, TEST_RELEASE_NAME);
    verify();
  }

  @Test
  public void testSummarizerIdempotence() throws IOException {
    // Execute twice
    service.summarize(TEST_JOB_ID, TEST_RELEASE_NAME);
    service.summarize(TEST_JOB_ID, TEST_RELEASE_NAME);
    verify();
  }

  @Test
  public void testDuplicateKey() throws Exception {
    // See https://github.com/bguerout/jongo/issues/147
    val collection = getTargetJongo().getCollection("test");

    collection.insert("{friends:1}");
    ObjectNode value = collection.findOne("{friends:1}").as(ObjectNode.class);
    collection.save(value);

    assertThat(collection.count()).isEqualTo(1);
  }

  private void verify() {
    verifyDonors();
    verifyProjects();
    verifyGenes();
    verifyObservations();
    verifyReleases();
  }

  private void verifyDonors() {
    MongoCollection donors = getTargetCollection(DONOR_COLLECTION);

    assertThat(donors.count()).isEqualTo(TEST_DATA.size());

    for (val donorId : TEST_DATA.keySet()) {
      JsonNode donor = donors.findOne("{" + DONOR_ID + ": '" + donorId + "'}").as(JsonNode.class);
      assertThat(donor).isEqualTo(TEST_DATA.get(donorId));
    }

  }

  private void verifyProjects() {
    MongoCollection projects = getTargetCollection(PROJECT_COLLECTION);

    assertThat(projects.count()).isEqualTo(1);

    String projectId = "project1";
    JsonNode project = projects.findOne("{" + PROJECT_ID + ": '" + projectId + "'}").as(JsonNode.class);

    // @formatter:off
    assertThat(project).isEqualTo($(
      "{" +
        "_id:'1'," +
        PROJECT_ID + ":'" + projectId + "'," +
        
        // Project-donor summaries
        PROJECT_SUMMARY + ":{" +
      		AVAILABLE_DATA_TYPES + ":['ssm','cnsm']," +
        
          getTestedTypeCountFieldName(SSM_TYPE)  + ":2," +
          getTestedTypeCountFieldName(CNSM_TYPE) + ":2," +
          getTestedTypeCountFieldName(CNGV_TYPE) + ":0," +
          getTestedTypeCountFieldName(SGV_TYPE)  + ":0," +
          getTestedTypeCountFieldName(STGV_TYPE) + ":0," +
          getTestedTypeCountFieldName(STSM_TYPE) + ":0," +
          getTestedTypeCountFieldName(EXP_ARRAY_TYPE)  + ":0," +
          getTestedTypeCountFieldName(EXP_SEQ_TYPE)  + ":0," +
          getTestedTypeCountFieldName(METH_ARRAY_TYPE)  + ":0," +
          getTestedTypeCountFieldName(METH_SEQ_TYPE)  + ":0," +
          getTestedTypeCountFieldName(MIRNA_SEQ_TYPE)  + ":0," +         
          getTestedTypeCountFieldName(JCN_TYPE)  + ":0," +
          getTestedTypeCountFieldName(PEXP_TYPE) + ":0," +
      		
          PROJECT_SUMMARY_STATE   + ":'live'," +
      		TOTAL_DONOR_COUNT   + ":3," +
      		TOTAL_SPECIMEN_COUNT + ":2," +
      		TOTAL_SAMPLE_COUNT  + ":2," +
      		
          PROJECT_SUMMARY_REPOSITORY + ":[" +        
            "'CGHub','EGA'" +
          "]," +      		
          EXPERIMENTAL_ANALYSIS_PERFORMED_DONOR_COUNT + ":{" + 
            "VALIDATION:2," +
            "WXS:2" +
          "}," +        		
          EXPERIMENTAL_ANALYSIS_PERFORMED_SAMPLE_COUNT + ":{" +  
            "VALIDATION:2," +           
            "WXS:2" +
          "}," +        		
          AVAILABLE_EXPERIMENTAL_ANALYSIS_PERFORMED + ":[" +
             "'VALIDATION', 'WXS'" +
          "]" + 
    		"}," +
      		
    		"name:'Project 1 Name'," +
    		"primary_site:'Primary Site 1'," +
    		"pubmed_ids:['pubmed_id1','pubmed_id2']" +
  		"}"
    ));
    // @formatter:on

  }

  private void verifyGenes() {
    MongoCollection genes = getTargetCollection(GENE_COLLECTION);

    assertThat(genes.count()).isEqualTo(3);

    String geneId = "gene2";
    JsonNode gene = genes.findOne("{" + GENE_ID + ": '" + geneId + "'}").as(JsonNode.class);

    // @formatter:off    
    assertThat(gene).isEqualTo($(
      "{" +
        "_id:'2'," +    
        GENE_ID + ":'" + geneId + "'," +
        
        // Gene-donor summaries
    		GENE_DONORS + ":[{" +
      		GENE_DONOR_DONOR_ID + ":'donor1'," +
          GENE_DONOR_SUMMARY + ":{" +        
            SSM_TYPE.getSummaryFieldName() + ":2," +
          
            CNSM_TYPE.getSummaryFieldName() + ":true," +
            CNGV_TYPE.getSummaryFieldName() + ":false," +
            SGV_TYPE.getSummaryFieldName() + ":false," +
            STGV_TYPE.getSummaryFieldName() + ":false," +
            STSM_TYPE.getSummaryFieldName() + ":false," +           
            EXP_ARRAY_TYPE.getSummaryFieldName()   + ":false," +
            EXP_SEQ_TYPE.getSummaryFieldName()   + ":false," +
            METH_ARRAY_TYPE.getSummaryFieldName()   + ":false," +
            METH_SEQ_TYPE.getSummaryFieldName()   + ":false," +
            MIRNA_SEQ_TYPE.getSummaryFieldName()   + ":false," +
            JCN_TYPE.getSummaryFieldName()  + ":false," +
            PEXP_TYPE.getSummaryFieldName() + ":false" +
          "}" +                   
  		  "}]," +
          
  		  // Gene-project summaries
  		  GENE_PROJECTS + ":[{" +
    		  GENE_PROJECT_PROJECT_ID + ":'project1'," +
  		    GENE_PROJECT_SUMMARY + ":{" +        
    		    AFFECTED_DONOR_COUNT + ":1," +
    		    AVAILABLE_DATA_TYPES + ":['ssm','cnsm']" +
          "}" +           		    
		    "}]," +
  		    
        "symbol:'G2'" +
	    "}"
    ));
    // @formatter:on
  }

  private void verifyObservations() {
    val observations = getTargetCollection(OBSERVATION_COLLECTION);

    assertThat(observations.count()).isEqualTo(3);

    val observationId = "515b327bb0c63b25e6da126d";
    val observation =
        observations.findOne("{" + OBSERVATION_ID + ": #}", new ObjectId(observationId)).as(JsonNode.class);

    val objectId = new ObjectId(observationId);

    // @formatter:off
    val expected = (ObjectNode)$(
      "{" +
        OBSERVATION_DONOR_ID + ":'donor1'," +
        OBSERVATION_MUTATION_ID + ":'MU1'," +
        OBSERVATION_TYPE + ":'ssm'," +
        
        OBSERVATION_CONSEQUENCES + ":[" +
          "{" +
            OBSERVATION_CONSEQUENCES_GENE_ID + ":'gene1'," +
            OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID + ":'transcript1'," +
            OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE + ":'consequence_type1'" +
          "}," +
          "{" +
            OBSERVATION_CONSEQUENCES_GENE_ID + ":'gene1'," +
            OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID + ":'transcript1'," +
            OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE + ":'consequence_type1'" +
          "}," +            
          "{" +
            OBSERVATION_CONSEQUENCES_GENE_ID + ":'gene2'," +
            OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID + ":'transcript2'," +
            OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE + ":'consequence_type1'" +
          "}" +
        "]," +
          
        // Observation-consequence types summaries
        OBSERVATION_CONSEQUENCE_TYPES + ":['consequence_type1']" +
      "}"
    );
    // @formatter:on

    // Need to replicate the exact ObjectId structure and types
    expected.put(OBSERVATION_ID, new POJONode(objectId));

    assertThat(observation).isEqualTo(expected);
  }

  private void verifyReleases() {
    val releases = getTargetCollection(RELEASE_COLLECTION);

    assertThat(releases.count()).isEqualTo(1);

    val releaseId = TEST_JOB_ID;
    val release = releases.findOne("{" + RELEASE_ID + ": #}", releaseId).as(JsonNode.class);

    // @formatter:off
    assertThat(release.toString()).isEqualTo($(
      "{" +
        MONGO_INTERNAL_ID + ":'" + releaseId + "'," + 
        RELEASE_ID + ":'" + releaseId + "'," + 
        RELEASE_NAME + ":'" + TEST_RELEASE_NAME + "'," + 
        RELEASE_NUMBER + ":" + TEST_RUN_NUMBER + "," + 
        RELEASE_DATE + ":'1969-12-31T19:00:00.000-05:00'," + 
        RELEASE_PROJECT_COUNT + ":1," + 
        RELEASE_LIVE_PROJECT_COUNT + ":1," + 
        RELEASE_PRIMARY_SITE_COUNT + ":1," + 
        RELEASE_DONOR_COUNT + ":3," + 
        RELEASE_LIVE_DONOR_COUNT + ":2," + 
        RELEASE_SPECIMEN_COUNT + ":2," + 
        RELEASE_SAMPLE_COUNT + ":2," + 
        RELEASE_SSM_COUNT + ":2," + 
        RELEASE_MUTATED_GENE_COUNT  + ":3" + 
      "}"
    ).toString());
    // @formatter:on
  }

  private void importData() {
    MongoImporter importer = new MongoImporter(getSourceDirectory(), getTargetDatabase());
    importer.execute();
  }

  @SneakyThrows
  private void configure() {
    log.info("Using releaseUri: {}", getTargetDatabaseUri());

    mongoUri = getTargetDatabaseUri();
    DateTimeUtils.setCurrentMillisFixed(0);

    // Create class under test
    service = new SummarizerService(mongoUri);
  }

  private MongoCollection getTargetCollection(ReleaseCollection collection) {
    return getTargetJongo().getCollection(collection.getId());
  }

  @SneakyThrows
  private Jongo getTargetJongo() {
    return new Jongo(getTargetDatabase());
  }

  private File getSourceDirectory() {
    return new File(Tests.TEST_FIXTURES_DIR);
  }

  private DB getTargetDatabase() {
    return embeddedMongo.getMongo().getDB(getDatabaseName());
  }

  private String getTargetDatabaseUri() {
    return format("mongodb://localhost:%s/", embeddedMongo.getPort());
  }

  private String getDatabaseName() {
    return TEST_JOB_ID;
  }

}
