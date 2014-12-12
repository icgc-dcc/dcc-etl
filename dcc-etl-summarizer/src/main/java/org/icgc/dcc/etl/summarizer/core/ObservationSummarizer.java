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

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatRate;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.bson.types.ObjectId;
import org.icgc.dcc.etl.summarizer.repository.ReleaseRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.google.common.base.Stopwatch;

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

  private static final int BATCH_SIZE = 100000;

  private static final int COUNTER_THRESHOLD = 10000;

  public ObservationSummarizer(ReleaseRepository repository) {
    super(repository);
  }

  @Override
  public void summarize() {
    // DCC-1039: Batch query results to avoid MongoDB's 16MB document limit
    int count = 0, offset = 0;
    List<JsonNode> batch;
    Stopwatch watch = Stopwatch.createStarted();
    do {
      batch = repository.getObservationConsequenceTypes(offset, BATCH_SIZE);

      for (JsonNode result : batch) {
        ObjectId observationId = getObservationId(result);
        JsonNode consequenceTypes = result.withArray("consequenceTypes");

        repository.setObservationSummary(observationId, consequenceTypes);

        if (++count % COUNTER_THRESHOLD == 0) {
          log.info("Summarized {} observation(s) ({} docs/s)", //
              formatCount(count), formatRate(rate(COUNTER_THRESHOLD, watch)));
        }
      }

      offset += BATCH_SIZE;
    } while (batch.size() == BATCH_SIZE);
  }

  private ObjectId getObservationId(JsonNode result) {
    POJONode pojoNode = (POJONode) result.get("observationId");

    return (ObjectId) pojoNode.getPojo();
  }

}
