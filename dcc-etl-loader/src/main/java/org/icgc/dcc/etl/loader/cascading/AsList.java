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

import static org.icgc.dcc.common.hadoop.cascading.TupleEntries.remove;

import org.icgc.dcc.common.hadoop.cascading.operation.BaseAggregator;

import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * An {@code Aggregator} that produces a {@code Tuple} of all {@code TupleEntry} that passes by, effectively producing a
 * list of all {@code TupleEntry}.
 * <p>
 * So for a given group, if you have a list of {@code TupleEntry}:<br/>
 * {"f1"="v1.1", "f2"="v1.2"},<br/>
 * {"f1"="v2.1", "f2"="v2.2"},<br/>
 * ...<br/>
 * Then the resulting {@code Tuple} will contain a {@code Tuple} which contains (again, per grouping):<br/>
 * "ssm_s"={{"f1"="v1.1", "f2"="v1.2"}, {"f1"="v2.1", "f2"="v2.2"}, ...}<br/>
 * <p>
 * Note that the operation removes the redundant fields provided from the resulting list (typically join fields).
 */
public final class AsList extends BaseAggregator<Tuple> {

  private final Fields redundantFields;

  public AsList(Fields fields, Fields redundantFields) {
    super(fields);
    this.redundantFields = redundantFields;
  }

  @Override
  public void start(@SuppressWarnings("rawtypes") FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {

    // The resulting tuple to which sub-tuples will be added
    Tuple listTuple = new Tuple();
    Tuple context = new Tuple((Object) listTuple);

    aggregatorCall.setContext(context);
  }

  @Override
  public void aggregate(@SuppressWarnings("rawtypes") FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {

    // Grab the result tuple to extend
    Tuple context = aggregatorCall.getContext();
    Tuple listTuple = (Tuple) context.getObject(0);

    // Add all fields to it
    listTuple.add(
        remove(
            new TupleEntry(aggregatorCall.getArguments()), // Must copy in order to modify
            redundantFields));
  }

  @Override
  public void complete(@SuppressWarnings("rawtypes") FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {
    Tuple context = aggregatorCall.getContext();
    aggregatorCall.getOutputCollector().add(context);
  }

}