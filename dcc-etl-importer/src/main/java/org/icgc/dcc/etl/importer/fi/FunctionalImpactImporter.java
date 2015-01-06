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
package org.icgc.dcc.etl.importer.fi;

import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.importer.fi.core.FunctionalImpactCalculator.calculateImpact;

import java.net.UnknownHostException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.fi.CompositeImpactCategory;
import org.icgc.dcc.etl.importer.util.LongBatchQueryModifier;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * Runs the basic algorithm as outlined in: https://wiki.oicr.on.ca/display/DCCSOFT/SSM+functional+impact+annotation
 */
@Slf4j
@RequiredArgsConstructor
public class FunctionalImpactImporter {

  private static final String CONSEQUENCE = "consequence";
  private static final String CONSEQUENCE_TYPE = "consequence_type";
  private static final String FUNCTIONAL_IMPACT_PREDICTION = "functional_impact_prediction";
  private static final String FUNCTIONAL_IMPACT_PREDICTION_SUMMARY = "functional_impact_prediction_summary";

  @NonNull
  private final MongoClientURI mongoUri;

  @SneakyThrows
  public void execute() {
    val jongo = createJongo();
    val observationCollection = jongo.getCollection(OBSERVATION_COLLECTION.getId());

    val observations = readObservations(observationCollection);

    writeObservations(observationCollection, observations);
    observations.close();
  }

  private void writeObservations(MongoCollection observationCollection, MongoCursor<ObjectNode> observations) {
    long observationCounter = 0;
    long consequenceCounter = 0;

    log.info("Calculating functional impact predictions");
    for (val observation : observations) {
      observationCounter++;

      val consequences = (ArrayNode) observation.get(CONSEQUENCE);
      val impactSummary = Sets.<String> newHashSet();

      if (null != consequences && consequences.isArray() && consequences.size() > 0) {
        for (val consequence : consequences) {
          consequenceCounter++;
          val fiNode = consequence.get(FUNCTIONAL_IMPACT_PREDICTION);

          val functionalImpactPredictionSummary = calculateImpact(consequence.get(CONSEQUENCE_TYPE).asText(), fiNode);

          impactSummary.add(functionalImpactPredictionSummary);
          ((ObjectNode) consequence).put(FUNCTIONAL_IMPACT_PREDICTION_SUMMARY, functionalImpactPredictionSummary);
        }
      } else {
        impactSummary.add(CompositeImpactCategory.UNKNOWN.name());
      }

      if (impactSummary.size() > 0) {
        val summaryNode = JsonNodeFactory.instance.arrayNode();
        for (val item : impactSummary) {
          summaryNode.add(item.toString());
        }

        observation.put(FUNCTIONAL_IMPACT_PREDICTION_SUMMARY, summaryNode);
        observationCollection.save(observation);
      }

      if (observationCounter % 50000 == 0) {
        log.info("Processed {} observations, {} consequences", formatCount(observationCounter),
            formatCount(consequenceCounter));
      }
    }

    log.info("Processed {} observations, {} consequences", formatCount(observationCounter),
        formatCount(consequenceCounter));
  }

  private MongoCursor<ObjectNode> readObservations(MongoCollection observationCollection) {

    return observationCollection
        .find()
        .with(new LongBatchQueryModifier())
        .as(ObjectNode.class);

  }

  private Jongo createJongo() throws UnknownHostException {
    val database = mongoUri.getDatabase();
    val mongo = new MongoClient(mongoUri);
    val mongoDB = mongo.getDB(database);

    return new Jongo(mongoDB);
  }

}
