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
import static com.google.common.base.Preconditions.checkNotNull;
import static org.icgc.dcc.common.cascading.Fields2.checkFieldsCardinality;
import static org.icgc.dcc.common.cascading.Fields2.fields;
import static org.icgc.dcc.common.cascading.TupleEntries.getFirstString;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.cascading.operation.BaseFunction;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Translates {@link Fields} using the provided mappings.
 */
class CodesTranslator extends SubAssembly {

  /**
   * @param mappings A {@link Map} of {@link Fields} to their code-to-value mappings (String -> String).
   */
  CodesTranslator(Pipe pipe, @NonNull Map<String, Map<String, String>> mappings) {
    setTails(processRecursively(
        pipe,
        mappings.entrySet().iterator()));
  }

  private static Pipe processRecursively(Pipe pipe, Iterator<Entry<String, Map<String, String>>> entries) {
    if (entries.hasNext()) {
      val entry = entries.next();
      return processRecursively(
          new Each(
              pipe,

              // Field to translate
              fields(entry.getKey()),

              // Translator
              new Translate(
                  entry.getValue()), // Mapping

              REPLACE),
          entries);
    } else {
      return pipe;
    }
  }

  private static class Translate extends BaseFunction<Void> {

    /**
     * Code to value mapping.
     */
    private final Map<String, String> mapping;

    private Translate(@NonNull Map<String, String> mapping) {
      super(ARGS);
      this.mapping = mapping;
    }

    @Override
    public void operate(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        FunctionCall<Void> functionCall) {
      checkFieldsCardinality(functionCall.getArgumentFields(), 1);
      val code = checkNotNull(getFirstString(functionCall.getArguments()));
      functionCall
          .getOutputCollector()
          .add(new Tuple(tryTranslate(mapping, code)));
    }

    /**
     * Only translate if known code (may already be translated or converted to a replacement value for nulls for
     * instance)
     */
    private static String tryTranslate(Map<String, String> mapping, String code) {
      return firstNonNull(mapping.get(code), code);
    }

  }

}
