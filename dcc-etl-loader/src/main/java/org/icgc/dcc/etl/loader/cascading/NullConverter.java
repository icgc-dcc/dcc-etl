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
package org.icgc.dcc.etl.loader.cascading;

import static cascading.tuple.Fields.ARGS;
import static cascading.tuple.Fields.REPLACE;
import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Strings.emptyToNull;
import static org.icgc.dcc.common.cascading.Fields2.checkFieldsCardinality;
import static org.icgc.dcc.common.cascading.TupleEntries.getFirstString;

import java.util.Iterator;

import lombok.NonNull;

import org.icgc.dcc.common.cascading.operation.BaseFunction;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Concerts {@link Fields} that are null or empty to the replacement provided.
 */
class NullConverter extends SubAssembly {

  NullConverter(Pipe pipe, @NonNull Fields targetFields, String replacement) {
    setTails(processRecursively(
        pipe,
        getIterator(targetFields),
        replacement));
  }

  @SuppressWarnings("unchecked")
  private final Iterator<Comparable<?>> getIterator(Fields f) {
    return f.iterator();
  }

  private static Pipe processRecursively(Pipe pipe, Iterator<Comparable<?>> fields, String replacement) {
    if (fields.hasNext()) {
      return processRecursively(
          new Each(
              pipe,

              // Field to convert
              new Fields(fields.next()),

              // Convert
              new Convert(replacement),

              REPLACE),
          fields,
          replacement);
    } else {
      return pipe;
    }
  }

  private static class Convert extends BaseFunction<Void> {

    private final String replacement;

    private Convert(String replacement) {
      super(ARGS);
      this.replacement = replacement;
    }

    @Override
    public void operate(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        FunctionCall<Void> functionCall) {
      checkFieldsCardinality(functionCall.getArgumentFields(), 1);
      functionCall
          .getOutputCollector()
          .add(
              new Tuple(
                  firstNonNull(
                      emptyToNull(getFirstString(functionCall.getArguments())),
                      replacement)));
    }

  }

}
