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
package org.icgc.dcc.etl.loader.cascading;

import static cascading.tuple.Fields.ARGS;
import static cascading.tuple.Fields.REPLACE;
import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PACKAGE;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MARKING;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SSM_P_TYPE;
import static org.icgc.dcc.common.core.model.Marking.from;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;
import static org.icgc.dcc.common.hadoop.cascading.Tuples2.nestTuple;
import lombok.NoArgsConstructor;
import lombok.val;

import org.icgc.dcc.common.core.model.Marking;
import org.icgc.dcc.etl.loader.service.LoaderModel;
import org.icgc.dcc.common.hadoop.cascading.TupleEntries;
import org.icgc.dcc.common.hadoop.cascading.operation.BaseFilter;
import org.icgc.dcc.common.hadoop.cascading.operation.BaseFunction;

import cascading.flow.FlowProcess;
import cascading.operation.FilterCall;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.google.common.annotations.VisibleForTesting;

/**
 * Filters out "controlled" rows for the portal (not the exported files). This is used in 2 places however: while
 * counting rows in the summarization and right before persisting to mongo.
 */
public class ControlledRowsRedacting extends SubAssembly { // TODO: reword

  /**
   * See {@link ControlledRowsRedacting}.
   */
  public ControlledRowsRedacting(Pipe pipe) {
    setTails(new Each(
        new Each(
            pipe,
            new EntirelyControlledRowsFilter()),
        LoaderModel.Occurence.OBSERVATION_ARRAY_FIELD,
        new ControlledObservationsRedactor(),
        REPLACE));
  }

  /**
   * Removes {@link Tuple}s for which the "marking" field (a {@link String} representation of the enum {@link Marking})
   * is {@link Marking#CONTROLLED}.
   */
  @NoArgsConstructor(access = PACKAGE)
  @VisibleForTesting
  static final class EntirelyControlledRowsFilter extends BaseFilter<Void> {

    /**
     * See {@link EntirelyControlledRowsFilter}.
     */
    @Override
    public boolean isRemove(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        FilterCall<Void> filterCall) {
      val observations = (Tuple) filterCall.getArguments()
          .getObject(LoaderModel.Occurence.OBSERVATION_ARRAY_FIELD); // By design
      checkState(!observations.isEmpty()); // By design

      for (val item : observations) {
        val observation = (TupleEntry) item; // By design

        // Exit if one qualifies
        if (!getMarking(observation).isControlled()) {
          return false;
        }
      }

      return true;
    }
  }

  static final class ControlledObservationsRedactor extends BaseFunction<Void> {

    private ControlledObservationsRedactor() {
      super(ARGS);
    }

    @Override
    public void operate(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        FunctionCall<Void> functionCall) {
      val observations = (Tuple) functionCall.getArguments()
          .getObject(LoaderModel.Occurence.OBSERVATION_ARRAY_FIELD); // By design

      val redactionTuple = new Tuple();
      for (val item : observations) {
        val observation = (TupleEntry) item; // By design

        // Add observations that qualify (there should be at least one at this stage)
        if (!getMarking(observation).isControlled()) {
          redactionTuple.add(
              TupleEntries.clone(observation));
        }
      }
      checkState(!redactionTuple.isEmpty()); // By design (we just filtered out any rows that would produce such an
                                             // empty array)

      functionCall
          .getOutputCollector()
          .add(nestTuple(redactionTuple));
    }

  }

  private static Marking getMarking(TupleEntry observation) {
    val markingField = prefixedFields(SSM_P_TYPE, NORMALIZER_MARKING);
    val markingString = observation.getString(markingField);
    val marking = from(markingString);
    checkState(marking.isPresent(),
        "All observations are expected to have a '%s' field at this point, instead got: '%s'",
        NORMALIZER_MARKING, observation);
    return marking.get();
  }

}
