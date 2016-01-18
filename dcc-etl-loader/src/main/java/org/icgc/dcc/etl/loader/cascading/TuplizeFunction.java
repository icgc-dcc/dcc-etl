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

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.cascading.TupleEntries.getFieldNames;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.unprefixFieldName;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Supplemental.DONOR_ID_BASE_FIELD_NAME;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Supplemental.SUPPLEMENTAL_DONOR_ID_FIELD_NAME;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Supplemental.SUPPLEMENTAL_REST_FIELD_NAME;
import lombok.val;

import org.icgc.dcc.common.cascading.operation.BaseFunction;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Transforms entry a tuple mapping of id field and rest of fields.
 */
public class TuplizeFunction extends BaseFunction<Void> {

  public TuplizeFunction() {
    super(new Fields(SUPPLEMENTAL_DONOR_ID_FIELD_NAME, SUPPLEMENTAL_REST_FIELD_NAME));
  }

  @Override
  public void operate(
      @SuppressWarnings("rawtypes") FlowProcess flowProcess,
      FunctionCall<Void> functionCall) {

    val entry = functionCall.getArguments();
    val incomingFields = entry.getFields();
    val incomingTuple = entry.getTupleCopy();
    val outgoingTuple = new Tuple();

    for (val prefixedFieldName : getFieldNames(entry)) {
      val value = entry.getObject(new Fields(prefixedFieldName));
      val unprefixedFieldName = unprefixFieldName(prefixedFieldName);

      if (unprefixedFieldName.equals(DONOR_ID_BASE_FIELD_NAME)) {
        outgoingTuple.add(value);

        val donorIdField = new Fields(prefixedFieldName);
        incomingTuple.remove(incomingFields, donorIdField);
        val outgoingFields = incomingFields.subtract(donorIdField);
        val outgingTupleEntry = new TupleEntry(outgoingFields, incomingTuple);

        checkState(!outgoingTuple.isEmpty());
        checkState(outgingTupleEntry.size() != 0);

        outgoingTuple.add(outgingTupleEntry);
        break;
      }
    }

    functionCall
        .getOutputCollector()
        .add(outgoingTuple);
  }
}
