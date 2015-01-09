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
package org.icgc.dcc.etl.importer.fathmm.writer;

import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_AA_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCE_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_MUTATION_ID;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import lombok.val;

import org.icgc.dcc.common.core.model.ConsequenceType;
import org.icgc.dcc.etl.importer.fathmm.core.FathmmPredictor;
import org.icgc.dcc.etl.importer.util.AbstractJongoComponent;
import org.jongo.MongoCollection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.BiMap;
import com.google.common.collect.Lists;
import com.mongodb.MongoClientURI;

public class FathmmObservationWriter extends AbstractJongoComponent {

  private final MongoCollection observationCollection;

  public FathmmObservationWriter(MongoClientURI mongoUri) {
    super(mongoUri);

    this.observationCollection = getCollection(OBSERVATION_COLLECTION);
  }

  public int write(BiMap<String, String> transcripts, FathmmPredictor predictor, ObjectNode observation) {
    val consequences = (ArrayNode) observation.get(OBSERVATION_CONSEQUENCES);
    val consequenceList = Lists.<JsonNode> newArrayList();
    int consequenceCounter = 0;

    if (null != consequences && consequences.isArray()) {
      boolean hasFathmmScore = false;
      for (val consequence : consequences) {
        consequenceList.add(consequence);

        val aaMutation = consequence.get(OBSERVATION_CONSEQUENCES_AA_MUTATION);
        val transcriptId = consequence.get(OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID);
        val consequenceType = consequence.get(OBSERVATION_CONSEQUENCE_TYPES);

        if (null == aaMutation || null == transcriptId
            || !consequenceType.textValue().equals(ConsequenceType.MISSENSE_VARIANT.getId())) {
          continue;
        }

        val translationIdStr = transcripts.get(transcriptId.textValue());
        val aaMutationStr = aaMutation.textValue();
        if (null == translationIdStr) {
          continue;
        }

        val result = predictor.predict(translationIdStr, aaMutationStr);
        if (!result.isEmpty() && result.get("Score") != null) {

          if (consequence.get(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION) == null) {
            ((ObjectNode) consequence).put(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION,
                JsonNodeFactory.instance.objectNode());
          }

          val fathmmNode = JsonNodeFactory.instance.objectNode();
          fathmmNode.put("score", result.get("Score"));
          fathmmNode.put("prediction", result.get("Prediction"));
          fathmmNode.put("algorithm", "fathmm");

          ((ObjectNode) consequence.get(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION)).put(
              "fathmm", fathmmNode);

          hasFathmmScore = true;
        }
        consequenceCounter++;
      }

      if (hasFathmmScore) {
        val donorId = observation.get(OBSERVATION_DONOR_ID).textValue();
        val mutationId = observation.get(OBSERVATION_MUTATION_ID).textValue();

        observationCollection
            .update("{_donor_id:#, _mutation_id:#}", donorId, mutationId)
            .with("{$set:{consequence: #}}", consequenceList);
      }

    }

    return consequenceCounter;
  }

}
