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
package org.icgc.dcc.etl.loader.flow;

import static cascading.tuple.Fields.ALL;
import static cascading.tuple.Fields.ARGS;
import static cascading.tuple.Fields.REPLACE;
import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Occurence.CONSEQUENCE_FIELD;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Occurence.OCCURENCE_GROUP_BY_FIELDS;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Occurence.NO_VALUE_REPLACEMENT;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Occurence.OBSERVATION_ARRAY_FIELD;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Occurence.OCCURENCE_FIELDS;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Occurence.SECONDARY_SORT_FIELDS;
import static org.icgc.dcc.common.cascading.Fields2.cloneFields;
import static org.icgc.dcc.common.cascading.Tuples2.nestTuple;
import static org.icgc.dcc.common.cascading.Tuples2.sortTuples;

import java.util.Iterator;

import lombok.val;

import org.icgc.dcc.common.cascading.TupleEntries;
import org.icgc.dcc.common.cascading.operation.BaseBuffer;
import org.icgc.dcc.common.cascading.operation.BaseFunction;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;

/**
 * Nests observations within occurence.
 */
public class ObservationNesting extends SubAssembly {

  public ObservationNesting(Pipe pipe) {
    setTails(new Retain(
        new Each(
            new Every(
                new GroupBy(
                    pipe,
                    OCCURENCE_GROUP_BY_FIELDS,
                    SECONDARY_SORT_FIELDS),
                new Nest(OBSERVATION_ARRAY_FIELD),
                ALL),
            OCCURENCE_FIELDS
                .append(OBSERVATION_ARRAY_FIELD),
            new ObservationTrimmer(),
            REPLACE),
        OCCURENCE_FIELDS
            .append(OBSERVATION_ARRAY_FIELD)));
  }

  private static class Nest extends BaseBuffer<Void> {

    public Nest(Fields nestingField) {
      super(nestingField);
    }

    @Override
    public void operate(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        BufferCall<Void> bufferCall) {

      bufferCall
          .getOutputCollector()
          .add(nestEntries(bufferCall.getArgumentsIterator()));
    }

    private static Tuple nestEntries(Iterator<TupleEntry> entries) {
      val tuple = new Tuple();

      while (entries.hasNext()) {
        tuple.add(
            TupleEntries.clone(
                entries.next()));
      }

      return nestTuple(tuple);
    }

  }

  private static class ObservationTrimmer extends BaseFunction<Void> {

    public ObservationTrimmer() {
      super(ARGS);
    }

    @Override
    public void operate(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        FunctionCall<Void> functionCall) {

      val entry = functionCall.getArguments();
      val observations = (Tuple) entry.getObject(OBSERVATION_ARRAY_FIELD); // By design
      val observationIterator = observations.iterator();
      val firstObservation = (TupleEntry) observationIterator.next(); // By design

      functionCall
          .getOutputCollector()
          .add(
              combine(
                  firstObservation,
                  getTrimmedObservations(
                      firstObservation,
                      observationIterator)));
    }

    private static Tuple getTrimmedObservations(
        final TupleEntry firstObservation,
        final Iterator<Object> observationIterator) {

      boolean first = true;
      val trimmedObservations = new Tuple();

      while (first || observationIterator.hasNext()) {
        TupleEntry observation = first ?
            firstObservation :
            (TupleEntry) observationIterator.next();

        trimmedObservations.add(
            getTrimmedObservation(observation));

        if (!first) {
          checkConsistency(firstObservation, observation);
        }

        first = false;
      }

      return trimmedObservations;
    }

    private static TupleEntry getTrimmedObservation(TupleEntry observation) {
      val trimmedObservationFields = cloneFields(observation.getFields())
          .subtract(OCCURENCE_FIELDS);

      return new TupleEntry(
          trimmedObservationFields,
          new Tuple(
              Tuples.extract(
                  observation,
                  trimmedObservationFields)));
    }

    /**
     * All the top-level fields are expected to be the same for each observation.
     */
    private static void checkConsistency(TupleEntry firstObservation, TupleEntry subsequentObservation) {
      for (Comparable<?> occurenceField : OCCURENCE_FIELDS) {
        if (isConsequenceArray(occurenceField)) {
          val firstConsequences = sortTuples((Tuple) checkNotNull( // By design
              firstObservation.getObject(occurenceField)));
          val redundantConsequences = sortTuples((Tuple) checkNotNull( // By design
              subsequentObservation.getObject(occurenceField)));

          checkState(firstConsequences.equals(redundantConsequences),
              "Mismatch for field '%s': '%s' != '%s'",
              occurenceField, firstConsequences, redundantConsequences);
        } else {
          val firstValue = firstNonNull(firstObservation.getString(occurenceField), NO_VALUE_REPLACEMENT);
          val redundantValue = firstNonNull(subsequentObservation.getString(occurenceField), NO_VALUE_REPLACEMENT);
          checkState(firstValue.equals(redundantValue),
              "Mismatch for field '%s': '%s' != '%s'",
              occurenceField, firstValue, redundantValue);
        }
      }
    }

    private static Tuple combine(TupleEntry firstObservation, Tuple trimmedObservations) {
      val occurenceFields = new Tuple(
          Tuples.extract(
              firstObservation,
              OCCURENCE_FIELDS));
      occurenceFields.add(trimmedObservations);

      return occurenceFields;
    }

    private static boolean isConsequenceArray(Comparable<?> occurenceField) {
      return CONSEQUENCE_FIELD.equals(new Fields(occurenceField));
    }

  }

}
