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

import static cascading.tuple.Fields.ALL;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static org.icgc.dcc.common.cascading.Tuples2.nestTuple;
import static org.icgc.dcc.common.core.model.FeatureTypes.withRawSequenceData;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SAMPLE_TYPE;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;
import static org.icgc.dcc.etl.loader.flow.planner.PipeNames.getRawSequenceDataInfoPipeName;
import static org.icgc.dcc.etl.loader.service.LoaderModel.RawSequence.AVAILABLE_RAW_SEQUENCE_DATA_FIELD;
import static org.icgc.dcc.etl.loader.service.LoaderModel.RawSequence.META_FILES_SAMPLE_ID_FIELD;
import static org.icgc.dcc.etl.loader.service.LoaderModel.RawSequence.META_FILE_RETAIN_FIELDS;
import static org.icgc.dcc.etl.loader.service.LoaderModel.RawSequence.MISSING_VALUE_REPLACEMENT;
import static org.icgc.dcc.etl.loader.service.LoaderModel.RawSequence.RAW_SEQUENCE_DATA_FIELDS_MAPPING;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.cascading.SubAssemblies.NullReplacer.EmptyTupleNullReplacer;
import org.icgc.dcc.common.cascading.TupleEntries;
import org.icgc.dcc.common.cascading.operation.BaseBuffer;
import org.icgc.dcc.common.core.collect.SerializableMaps;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.Identifiable.Identifiables;
import org.icgc.dcc.common.core.model.SpecialValue;
import org.icgc.dcc.common.core.model.SubmissionModel;
import org.icgc.dcc.common.core.util.Strings2;
import org.icgc.dcc.etl.loader.mongodb.TupleEntryToDBObjectTransformer;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Adds raw sequence data based on meta files to sample records.
 */
@Slf4j
public class RawSequenceDataInfo extends SubAssembly {

  private final String projectKey;
  private final Set<FeatureType> availableFeatureTypes;
  private final Map<FeatureType, Map<String, Map<String, String>>> guaranteedFieldsMappings;

  public RawSequenceDataInfo(
      @NonNull Pipe pipe,
      @NonNull final String projectKey,
      @NonNull final Set<FeatureType> availableFeatureTypes,
      @NonNull final Map<FeatureType, Map<String, Map<String, String>>> guaranteedFieldsMappings) {

    this.projectKey = projectKey;
    this.availableFeatureTypes = availableFeatureTypes;
    this.guaranteedFieldsMappings = guaranteedFieldsMappings;

    log.info("Adding raw sequence data info for project '{}'", projectKey);
    setTails(addRawSequenceData(pipe));
  }

  private Pipe addRawSequenceData(Pipe pipe) {
    return

    // Discard redundant join field
    new Discard(

        // Replace nulls resulting from left join with an empty array
        new EmptyTupleNullReplacer(

            AVAILABLE_RAW_SEQUENCE_DATA_FIELD,

            // Perform join between "sample" and the processed "meta" files info
            new CoGroup(

                // LHS pipe
                pipe,

                // LHS join field
                prefixedFields(SAMPLE_TYPE, SUBMISSION_ANALYZED_SAMPLE_ID),

                // RHS pipe
                transformAvailableMetaDataInfo(availableFeatureTypes),

                META_FILES_SAMPLE_ID_FIELD,

                // Joiner
                new LeftJoin())
        ),

        META_FILES_SAMPLE_ID_FIELD);
  }

  private Pipe transformAvailableMetaDataInfo(final Set<FeatureType> availableFeatureTypes) {
    return

    // Only retain fields of interest
    new Retain(

        // Nest the grouped tuples under a new field: "available_raw_sequence_data"
        new Every(

            // Group by sample ID since we ultimately nest it under sample
            new GroupBy(

                // Combine meta data for all available feature types
                combineAvailableMetaDataInfo(availableFeatureTypes),

                META_FILES_SAMPLE_ID_FIELD),

            // Nester (providing field under which to nest the raw sequence info in samples)
            // new MyAggregator(AVAILABLE_RAW_SEQUENCE_DATA_FIELD)),
            new Nester(AVAILABLE_RAW_SEQUENCE_DATA_FIELD)),

        // Retain fields: join field that will be used + new raw sequence field that nests the info
        META_FILES_SAMPLE_ID_FIELD
            .append(AVAILABLE_RAW_SEQUENCE_DATA_FIELD));
  }

  private Pipe combineAvailableMetaDataInfo(final Set<FeatureType> availableFeatureTypes) {
    return

    // Discard potential duplicates (TODO)
    new Unique(

        // Merge pipes from the applicable feature types
        new Merge(

            // Transform to Pipe array (cascading requirement)
            toArray(

                // Produce a pipe per feature type
                transform(

                    // Exclude types for which there is no raw sequence data
                    withRawSequenceData(availableFeatureTypes),

                    // Transformation: feature type -> pipe
                    new Function<FeatureType, Pipe>() {

                      @Override
                      public Pipe apply(FeatureType featureType) {
                        return extractFeatureTypeMetaDataInfo(featureType);
                      }

                    }),

                Pipe.class)),

        // Uniquify using all fields
        ALL);
  }

  private Pipe extractFeatureTypeMetaDataInfo(FeatureType availableFeatureType) {
    return

    // Translates fields
    new CodesTranslator(

        // Convert nulls/empty strings to "N/A"
        new NullConverter(

            // Only keep fields of interest
            new Retain(

                // Create head pipe
                new Pipe(
                    getPipeName(availableFeatureType)),

                META_FILE_RETAIN_FIELDS),

            // Replace nulls/empty string for every fields
            ALL,
            MISSING_VALUE_REPLACEMENT),

        guaranteedFieldsMappings.get(availableFeatureType)

    );
  }

  private String getPipeName(FeatureType featureType) {
    return getRawSequenceDataInfoPipeName(Identifiables.fromString(projectKey), featureType);
  }

  @VisibleForTesting
  static class Nester extends BaseBuffer<Void> {

    public Nester(Fields nestingField) {
      super(nestingField);
    }

    @Override
    public void operate(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        BufferCall<Void> bufferCall) {

      bufferCall
          .getOutputCollector()
          .add(nestEntries(
              bufferCall.getArgumentsIterator(),
              bufferCall.getGroup().getFields()));
    }

    private static Tuple nestEntries(Iterator<TupleEntry> entries, Fields groupFields) {
      val tuple = new Tuple();
      while (entries.hasNext()) {
        tuple.add(

            // Remove group field(s)
            TupleEntries.remove(

                // Clone entry
                TupleEntries.clone(
                    entries.next()),

                groupFields));
      }

      return nestTuple(tuple);
    }

  }

  /**
   * Only returns mapping relevant to the raw sequence data.
   */
  public static Map<FeatureType, Map<String, Map<String, String>>> filterRawSequencingMappings(
      @NonNull final SubmissionModel submissionModel,
      @NonNull final Set<FeatureType> availableFeatureTypes) {

    return ImmutableMap.copyOf(SerializableMaps.asMap(
        availableFeatureTypes,
        new Function<FeatureType, Map<String, Map<String, String>>>() {

          @Override
          public Map<String, Map<String, String>> apply(FeatureType featureType) {
            return SerializableMaps.filterKeys(
                submissionModel.getCodeLists(featureType.getMetaFileType()),
                in(RAW_SEQUENCE_DATA_FIELDS_MAPPING.keySet()));
          }

        }));
  }

  public static class DBObjectTransformer implements TupleEntryToDBObjectTransformer {

    @Override
    public DBObject transformEntry(TupleEntry entry) {
      val builder = new BasicDBObjectBuilder();
      for (val fieldName : RAW_SEQUENCE_DATA_FIELDS_MAPPING.keySet()) {
        builder.add(
            renameField(fieldName),
            getValue(entry, fieldName));
      }

      return builder.get();
    }

    /**
     * Temporary fix for DCC-2424.
     */
    private String getValue(TupleEntry entry, String fieldName) {
      val value = entry.getString(fieldName);
      return SpecialValue.FULL_MISSING_CODES.contains(value) ?
          Strings2.EMPTY_STRING : value;
    }

    private String renameField(String originalFieldName) {
      return RAW_SEQUENCE_DATA_FIELDS_MAPPING.get(originalFieldName);
    }

  }

}
