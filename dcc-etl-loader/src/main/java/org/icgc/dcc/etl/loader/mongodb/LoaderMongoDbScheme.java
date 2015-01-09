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
package org.icgc.dcc.etl.loader.mongodb;

import static org.icgc.dcc.common.core.model.DataType.DataTypes.isAggregatedType;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SSM_M_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SSM_P_TYPE;
import static org.icgc.dcc.common.core.model.TransformFileInfo.MUTATION_COLLECTION;
import static org.icgc.dcc.common.hadoop.cascading.Fields2.getFieldNames;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.generatedFields;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.unprefixFieldName;
import static org.icgc.dcc.etl.loader.service.LoaderModel.getCollection;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.BusinessKeys;
import org.icgc.dcc.common.core.model.DataType;
import org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames;
import org.icgc.dcc.common.core.model.SubmissionModel;
import org.icgc.dcc.common.hadoop.cascading.taps.MongoDbTap.MongoDbScheme;
import org.icgc.dcc.etl.loader.mongodb.LoaderHandler.HandlingType;
import org.icgc.dcc.etl.loader.mongodb.LoaderHandler.Nesting;
import org.icgc.dcc.etl.loader.mongodb.LoaderHandler.Redacting;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.google.common.base.Throwables;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * Converts the incoming {@code Tuple} into a {@code DBObject} according to the {@code Schema} and persists it in the
 * collection with the same name as the record's name.
 */
@Slf4j
public class LoaderMongoDbScheme extends MongoDbScheme {

  private final SubmissionModel submissionModel;
  private final DataType dataType;

  private transient DBCollection mutationDbCollection;

  public LoaderMongoDbScheme(
      @NonNull final SubmissionModel submissionModel,
      @SuppressWarnings("all") @NonNull final DataType dataType,
      @NonNull final String databaseName) {
    super(databaseName, dataType.getId(), null); // TODO: use optional
    this.submissionModel = submissionModel;
    this.dataType = dataType;
  }

  @Override
  protected String getCollectionName() { // TODO: should be reusing flow planner's
    return getCollection(dataType).getId();
  }

  @Override
  protected void preSinkHook(SinkCall<Void, Object> sinkCall) {
    if (isAggregatedType(dataType)) {
      log.debug("Setting collection '{}'", MUTATION_COLLECTION);
      this.mutationDbCollection = getDbCollection(sinkCall, databaseName, MUTATION_COLLECTION);
    }
  }

  @Override
  protected void persistHook(SinkCall<Void, Object> sinkCall, TupleEntry entry) {
    if (isAggregatedType(dataType)) {
      persistMutation(sinkCall, entry);
    }
  }

  @SneakyThrows
  private void persistMutation(SinkCall<Void, Object> sinkCall, TupleEntry entry) {
    DBObject mutationDbObject = extractMutation(entry, mutationFields());
    try {
      checkResult(upsert(mutationDbCollection, mutationDbObject));
    } catch (MongoException e) {
      log.error("{} caught while trying to upsert mutation {}",
          e.getClass().getSimpleName(), mutationDbObject);
      Throwables.propagate(e);
    }

  }

  /**
   * Returns the {@link Fields} for a mutation.
   * <p>
   * Careful to always keep this in sync with {@link MongoDbProcessing#MUTATION_FIELDS}.
   */
  private Fields mutationFields() {
    return prefixedFields(SSM_M_TYPE, BusinessKeys.MUTATION_META_IDENTIFYING_PART)
        .append(prefixedFields(SSM_P_TYPE, BusinessKeys.MUTATION_PRIMARY_IDENTIFYING_PART))
        .append(prefixedFields(SSM_P_TYPE, BusinessKeys.MUTATION_PRIMARY_REDUNDANT_INFO_PART))
        .append(generatedFields(IdentifierFieldNames.SURROGATE_MUTATION_ID)); // They should all be at the same level
  }

  private DBObject extractMutation(TupleEntry entry, Fields mutationBusinessKeyFields) {
    BasicDBObjectBuilder builder = new BasicDBObjectBuilder();

    for (String fieldName : getFieldNames(mutationBusinessKeyFields)) {
      builder.add(
          unprefixFieldName(fieldName),
          entry.getObject(fieldName));
    }

    return builder.get();
  }

  @Override
  public void sourceConfInit(FlowProcess<Object> flowProcess, Tap<Object, Void, Object> tap, Object conf) {
  }

  @Override
  public void sinkConfInit(FlowProcess<Object> flowProcess, Tap<Object, Void, Object> tap, Object conf) {
  }

  @Override
  protected DBObject convert(TupleEntry entry) {
    return new LoaderHandler(
        submissionModel,
        new HandlingType(
            dataType.getTopLevelFileType(),
            Nesting.JOIN),
        Redacting.SENSITIVE_FIELDS)
        .transformEntry(entry);
  }

}
