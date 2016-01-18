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

import org.icgc.dcc.common.core.model.DataType;
import org.icgc.dcc.common.core.model.SubmissionModel;
import org.icgc.dcc.etl.loader.mongodb.LoaderHandler;
import org.icgc.dcc.etl.loader.mongodb.LoaderHandler.HandlingType;
import org.icgc.dcc.etl.loader.mongodb.LoaderHandler.Nesting;
import org.icgc.dcc.etl.loader.mongodb.LoaderHandler.Redacting;
import org.icgc.dcc.common.cascading.operation.BaseFunction;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.mongodb.DBObject;

/**
 * Transforms nested tuples into a JSON string representation.
 */
public class HandlerFunction extends BaseFunction<Void> {

  /**
   * Field for the json string representation of documents (one per tuple).
   */
  public static final Fields DOCUMENT_FIELD = new Fields("_document");

  private final SubmissionModel submissionModel;
  private final DataType dataType;

  public HandlerFunction(
      SubmissionModel submissionModel,
      DataType dataType) {
    super(DOCUMENT_FIELD);
    this.submissionModel = submissionModel;
    this.dataType = dataType;
  }

  @Override
  public void operate(
      @SuppressWarnings("rawtypes") FlowProcess flowProcess,
      FunctionCall<Void> functionCall) {
    TupleEntry entry = functionCall.getArguments();
    DBObject dbObject = new LoaderHandler(
        submissionModel,
        new HandlingType(
            dataType.getTopLevelFileType(),
            Nesting.JOIN),
        Redacting.NONE)
        .transformEntry(entry);
    String json = dbObject.toString(); // For now the string representation is fine
    functionCall.getOutputCollector().add(new Tuple(json));
  }

}
