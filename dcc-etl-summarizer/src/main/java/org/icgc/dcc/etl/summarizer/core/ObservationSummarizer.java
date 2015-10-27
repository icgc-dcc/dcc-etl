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

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.common.core.model.FieldNames.MONGO_INTERNAL_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatRate;

import org.bson.types.ObjectId;
import org.elasticsearch.common.collect.Sets;
import org.icgc.dcc.etl.summarizer.repository.ReleaseRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.POJONode;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Summarizes {@code Observation} collection information.
 * <p>
 * Updates {@code Observation} documents to be of the form:
 * 
 * <pre>
 * {
 *   ...
 *   "consequence_type" : [ "consequence_type1" ],
 *   ...
 * }
 * </pre>
 */
@Slf4j
public class ObservationSummarizer extends AbstractSummarizer {

  private static final int COUNTER_THRESHOLD = 10000;

  public ObservationSummarizer(ReleaseRepository repository) {
    super(repository);
  }

  @Override
  public void summarize() {
    val watch = createStarted();

    int count = 0;
    for (val result : repository.getObservationConsequenceTypes()) {
      val observationId = getObservationId(result);
      val observationConsequences = getObservationConsequences(result);

      val consequenceTypes = Sets.<String> newHashSet();
      for (val observationConsequence : observationConsequences) {
        val consequenceType = getConsequenceType(observationConsequence);

        consequenceTypes.add(consequenceType);
      }

      repository.setObservationSummary(observationId, consequenceTypes);

      if (++count % COUNTER_THRESHOLD == 0) {
        log.info("Summarized {} observation(s) ({} docs/s)",
            formatCount(count), formatRate(rate(COUNTER_THRESHOLD, watch)));
      }
    }
  }

  private static ObjectId getObservationId(JsonNode result) {
    val pojoNode = (POJONode) result.get(MONGO_INTERNAL_ID);
    return (ObjectId) pojoNode.getPojo();
  }

  private static ArrayNode getObservationConsequences(JsonNode result) {
    return (ArrayNode) result.get(OBSERVATION_CONSEQUENCES);
  }

  private static String getConsequenceType(JsonNode observationConsequence) {
    return observationConsequence.path(OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE).textValue();
  }

}
