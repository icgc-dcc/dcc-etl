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
package org.icgc.dcc.etl.importer.fi.core;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.fi.ImpactPredictorType.FATHMM;
import static org.icgc.dcc.common.core.fi.ImpactPredictorType.MUTATION_ASSESSOR;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.fi.CompositeImpactCategory;
import org.icgc.dcc.common.core.fi.FathmmImpactCategory;
import org.icgc.dcc.common.core.fi.ImpactPredictorCategory;
import org.icgc.dcc.common.core.fi.ImpactPredictorType;
import org.icgc.dcc.common.core.fi.MutationAssessorImpactCategory;
import org.icgc.dcc.common.core.model.ConsequenceType;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;

@NoArgsConstructor(access = PRIVATE)
public final class FunctionalImpactCalculator {

  public static String calculateImpact(@NonNull String consequenceType, JsonNode impact) {
    val predictions = Maps.<ImpactPredictorType, ImpactPredictorCategory> newHashMap();

    if (null != impact) {
      for (val predictionNode : impact) {
        val algorithm = predictionNode.get("algorithm").asText();
        val prediction = predictionNode.get("prediction").asText();

        if (algorithm.equals("fathmm")) {
          predictions.put(FATHMM, FathmmImpactCategory.byId(prediction));
        } else if (algorithm.equals("mutation_assessor")) {
          predictions.put(MUTATION_ASSESSOR, MutationAssessorImpactCategory.byId(prediction));
        }
      }
    }

    val type = ConsequenceType.byId(consequenceType);
    val result = CompositeImpactCategory.calculate(type, predictions);

    return result.getId();
  }

}
