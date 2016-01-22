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
package org.icgc.dcc.etl.loader.mongodb;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.common.cascading.Fields2.field;
import static org.icgc.dcc.common.cascading.TupleEntries.OBJECT_TO_TUPLE_ENTRY_CAST;
import static org.icgc.dcc.common.cascading.TupleEntries.getFieldNames;
import static org.icgc.dcc.common.cascading.TupleEntries.getTuple;
import static org.icgc.dcc.common.cascading.TupleEntries.getTupleEntry;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.json.Jackson.DEFAULT;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.generatedFieldName;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.getPrefix;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixFieldNames;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFieldName;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.unprefixFieldName;
import static org.icgc.dcc.etl.loader.flow.SummaryHelper.getAvailableDataTypeField;
import static org.icgc.dcc.etl.loader.flow.SummaryHelper.getSummaryValueField;
import static org.icgc.dcc.etl.loader.service.LoaderModel.allFields;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Consequence.getConsequenceArrayInternalName;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Occurence.isObservationArrayInternalName;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Occurence.observationFields;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Occurence.occurenceFields;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Persistence.getNestedFileType;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Persistence.isGeneratedField;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Persistence.isJoinArrayInternalName;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Persistence.potentiallyRenameArray;
import static org.icgc.dcc.etl.loader.service.LoaderModel.RawSequence.AVAILABLE_RAW_SEQUENCE_DATA_FIELD;
import static org.icgc.dcc.etl.loader.service.LoaderModel.RawSequence.isAvailableRawSequenceDataField;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Summary.isSummaryField;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Supplemental.SPECIMEN_FIELD_NAME;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Supplemental.SPECIMEN_ID_FIELD_NAME;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Supplemental.SUPPLEMENTAL_MERGED_FIELD_NAME;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Supplemental.SUPPLEMENTAL_REST_FIELD_NAME;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Supplemental.isSupplementalField;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.icgc.dcc.common.core.model.ControlFieldsReference;
import org.icgc.dcc.common.core.model.DataType;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames;
import org.icgc.dcc.common.core.model.FileTypes.FileSubType;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.model.SubmissionModel;
import org.icgc.dcc.common.core.util.Optionals;
import org.icgc.dcc.etl.loader.cascading.RawSequenceDataInfo;
import org.icgc.dcc.etl.loader.flow.SummaryCollector;
import org.icgc.dcc.etl.loader.flow.planner.DonorRecordLoaderFlowPlanner;
import org.icgc.dcc.etl.loader.service.LoaderModel;
import org.icgc.dcc.etl.loader.service.LoaderModel.Occurence;
import org.icgc.dcc.etl.loader.service.LoaderModel.Submission;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.val;

/**
 * 
 */
@RequiredArgsConstructor
public class LoaderHandler implements TupleEntryToDBObjectTransformer {

  private final SubmissionModel submissionModel;
  private final HandlingType handlingType;
  private final Redacting redacting;

  @Override
  public DBObject transformEntry(TupleEntry entry) {
    val builder = new BasicDBObjectBuilder();

    val fileType = handlingType.getFileType();
    val optionalMetaFileType = getOptionalMetaFileType(handlingType);

    boolean hasSupplemental = false;

    val submissionFieldNames = filter(
        submissionModel.getFieldNames(fileType),
        relevantFields(handlingType));
    val prefixedSubmissionFieldNames = prefixFieldNames(fileType, submissionFieldNames);
    val prefixedSubmissionMetaFieldNames =
        getOptionalPrefixedSubmissionMetaFieldNames(submissionModel, optionalMetaFileType);

    for (val prefixedLoaderFieldName : getFieldNames(entry)) {
      val unprefixedLoaderFieldName = unprefixFieldName(prefixedLoaderFieldName);

      if (isNormalField(prefixedSubmissionFieldNames, prefixedLoaderFieldName)) {
        handleNormalValue(builder, entry, fileType, unprefixedLoaderFieldName);
      }

      else if (hasMetaType(handlingType)
          && isMetaField(prefixedSubmissionMetaFieldNames.get(), prefixedLoaderFieldName)) {
        handleMetaValue(builder, entry, optionalMetaFileType.get(), unprefixedLoaderFieldName);
      }

      else if (isGeneratedField(prefixedLoaderFieldName)) {
        if (isAvailableRawSequenceDataField(prefixedLoaderFieldName)) {
          handleRawSequenceDataInfoField(builder, entry, fileType);
        }

        else if (isSummaryField(prefixedLoaderFieldName)) {
          handleSummaryField(builder, entry, fileType);
        }

        else { // Any other generated field such as _type, _donor_id, ...
          handleOtherGeneratedField(builder, entry, fileType, unprefixedLoaderFieldName);
        }
      }

      else if (isObservationArrayInternalName(prefixedLoaderFieldName)) {
        handleObservationArray(builder, entry, fileType);
      }

      else if (isJoinArrayInternalName(prefixedLoaderFieldName)) {
        handleJoinArray(builder, entry, fileType, unprefixedLoaderFieldName);
      }

      else if (isSupplementalField(prefixedLoaderFieldName)) {
        // Supplemental data is handled after the rest of the document is prepared
        hasSupplemental = true;
      }

      else {
        throw new IllegalStateException(String.format("Unknown field: '%s' for '%s': '%s', '%s'",
            prefixedLoaderFieldName, handlingType, submissionFieldNames, entry.getFields()));
      }
    }

    if (hasSupplemental) {
      return handleSupplementalValue(builder, entry);
    }

    return builder.get();
  }

  private BasicDBObject handleSupplementalValue(BasicDBObjectBuilder builder, TupleEntry entry) {

    ObjectNode document = DEFAULT.convertValue(builder.get(), ObjectNode.class);
    val supplementalTuple = (Tuple) entry.getObject(new Fields(SUPPLEMENTAL_MERGED_FIELD_NAME));

    if (supplementalTuple != null) {

      for (Iterator<Object> i = supplementalTuple.iterator(); i.hasNext();) {
        val tupleEntry = (TupleEntry) i.next();
        val supplementalEntry = (TupleEntry) tupleEntry.getObject(new Fields(SUPPLEMENTAL_REST_FIELD_NAME));
        val fileType = determineSupplementalType(supplementalEntry);

        if (isSpecimenSupplementalType(fileType)) {
          handleSpecimenSupplementalFileType(document, supplementalEntry, fileType);
        } else if (isDonorSupplementalFileType(fileType)) {
          handleDonorSupplementalFileType(document, supplementalEntry, fileType);
        } else {
          throw new IllegalStateException(String.format("Unexpected file type: '%s'", fileType));
        }
      }
    }

    return DEFAULT.convertValue(document, BasicDBObject.class);
  }

  private void handleSpecimenSupplementalFileType(ObjectNode document, @NonNull final TupleEntry supplementalEntry,
      @NonNull final FileType fileType) {
    // Nest under specimen
    val entrySpecimenId = supplementalEntry.getString(prefixedFieldName(fileType, SPECIMEN_ID_FIELD_NAME));
    val specimens = document.path(SPECIMEN_FIELD_NAME);

    for (val specimen : specimens) {
      val specimenId = specimen.get(SPECIMEN_ID_FIELD_NAME).asText();

      if (specimenId.equals(entrySpecimenId)) {
        val existingValues = (ArrayNode) specimen.withArray(fileType.getId());
        existingValues.add(extractEntry(supplementalEntry));
        ((ObjectNode) specimen).put(fileType.getId(), existingValues);
      }
    }
  }

  private void handleDonorSupplementalFileType(ObjectNode document, @NonNull final TupleEntry supplementalEntry,
      @NonNull final FileType fileType) {
    val existingValues = document.withArray(fileType.getId());
    existingValues.add(extractEntry(supplementalEntry));
    document.put(fileType.getId(), existingValues);
  }

  private boolean isSpecimenSupplementalType(FileType fileType) {
    return fileType == FileType.BIOMARKER_TYPE || fileType == FileType.SURGERY_TYPE;
  }

  private boolean isDonorSupplementalFileType(FileType fileType) {
    return fileType == FileType.FAMILY_TYPE || fileType == FileType.EXPOSURE_TYPE
        || fileType == FileType.THERAPY_TYPE;
  }

  private static JsonNode extractEntry(TupleEntry entry) {

    JsonNode rootNode = DEFAULT.createObjectNode();

    for (val prefixedFieldName : getFieldNames(entry)) {
      val value = entry.getString(new Fields(prefixedFieldName));
      val unprefixedFieldName = unprefixFieldName(prefixedFieldName);
      ((ObjectNode) rootNode).put(unprefixedFieldName, value);
    }

    return rootNode;
  }

  private FileType determineSupplementalType(TupleEntry entry) {
    val prefixedFieldName = entry.getFields().get(0).toString();
    val prefix = getPrefix(prefixedFieldName);
    return FileType.from(prefix);
  }

  private void handleNormalValue(
      @NonNull final BasicDBObjectBuilder builder,
      @NonNull final TupleEntry entry,
      @NonNull final FileType fileType,
      @NonNull final String unprefixedFieldName) {

    handleValue(builder, entry, fileType, unprefixedFieldName);
  }

  private void handleMetaValue(
      @NonNull final BasicDBObjectBuilder builder,
      @NonNull final TupleEntry entry,
      @NonNull final FileType metaFileType,
      @NonNull final String unprefixedFieldName) {

    handleValue(builder, entry, metaFileType, unprefixedFieldName);
  }

  private void handleRawSequenceDataInfoField(
      @NonNull final BasicDBObjectBuilder builder,
      @NonNull final TupleEntry entry,
      @NonNull final FileType fileType) {

    checkState(fileType.isSample()); // By design
    builder.add(
        LoaderFieldNames.AVAILABLE_RAW_SEQUENCE_DATA,
        extractRawSequenceDataInfo(entry));
  }

  private void handleSummaryField(
      @NonNull final BasicDBObjectBuilder builder,
      @NonNull final TupleEntry entry,
      @NonNull final FileType fileType) {

    checkState(fileType.isDonor()); // By design
    builder.add(
        LoaderFieldNames.SUMMARY,
        extractSummary(getTupleEntry(
            entry,
            SummaryCollector.getStaticResultField())));
  }

  private void handleOtherGeneratedField(
      @NonNull final BasicDBObjectBuilder builder,
      @NonNull final TupleEntry entry,
      @NonNull final FileType fileType,
      @NonNull final String unprefixedFieldName) {

    builder.add(
        unprefixedFieldName,
        extractObjectValue(
            entry,
            generatedFieldName(unprefixedFieldName)));
  }

  private void handleJoinArray(
      @NonNull final BasicDBObjectBuilder builder,
      @NonNull final TupleEntry entry,
      @NonNull final FileType fileType,
      @NonNull final String unprefixedFieldName) {

    val subFileType = getNestedFileType(
        fileType,
        generatedFieldName(unprefixedFieldName));

    handleArray(
        builder, entry, subFileType, getConsequenceArrayInternalName(subFileType), Nesting.JOIN);
  }

  private void handleObservationArray(
      @NonNull final BasicDBObjectBuilder builder,
      @NonNull final TupleEntry entry,
      @NonNull final FileType fileType) {

    handleArray(
        builder, entry,
        Occurence.getObservationFileType(), Occurence.getObservationArrayInternalName(), Nesting.OBSERVATION);
  }

  private void handleArray(
      @NonNull final BasicDBObjectBuilder builder,
      @NonNull final TupleEntry entry,
      @NonNull final FileType nestedFileType,
      @NonNull final String prefixedArrayName,
      @NonNull final Nesting nesting) {

    builder.add(
        potentiallyRenameArray(nestedFileType),
        transfromTupleEntries(
            getTuple(entry, field(prefixedArrayName)),
            recursion(nestedFileType, nesting)));
  }

  private LoaderHandler recursion(
      @NonNull final FileType nestedFileType,
      @NonNull final Nesting nesting) {

    return new LoaderHandler(
        submissionModel,
        new HandlingType(nestedFileType, nesting),
        redacting);
  }

  private void handleValue(
      @NonNull final BasicDBObjectBuilder builder,
      @NonNull final TupleEntry entry,
      @NonNull final FileType fileType,
      @NonNull final String unprefixedFieldName) {

    val value = extractPotentiallySensitiveObjectValue(
        submissionModel, fileType, entry, unprefixedFieldName, redacting);

    if (stillContainsValue(value)) {
      builder.add(unprefixedFieldName, value);
    }
  }

  private static Optional<FileType> getOptionalMetaFileType(
      @NonNull final HandlingType handlingType) {

    return Optionals.ofCondition(
        hasMetaType(handlingType),
        new Supplier<FileType>() {

          @Override
          public FileType get() {
            return handlingType.getDataType().asFeatureType().getMetaFileType();
          }

        });
  }

  private static Optional<List<String>> getOptionalPrefixedSubmissionMetaFieldNames(
      @NonNull final SubmissionModel submissionModel,
      @NonNull final Optional<FileType> optionalMetaFileType) {
    return Optionals.ofCondition(
        optionalMetaFileType.isPresent(),
        new Supplier<List<String>>() {

          @Override
          public List<String> get() {

            return prefixFieldNames(
                optionalMetaFileType.get(),
                submissionModel.getFieldNames(optionalMetaFileType.get()));
          }

        });
  }

  /**
   * Transforms a {@link Tuple} containing {@link TupleEntry}s into a {@link DBObject} containing a list of
   * {@link DBObject}.
   */
  private static DBObject transfromTupleEntries(
      final Tuple tuple, final TupleEntryToDBObjectTransformer transformer) {
    val list = new BasicDBList();
    list.addAll(

        // Cast to list of DBObjects (potentially renames/excludes fields)
        newArrayList(transform(

            // Cast to list of tuple entries
            newArrayList(transform(tuple, OBJECT_TO_TUPLE_ENTRY_CAST)),

            // Apply transformation to DBObject
            new Function<TupleEntry, DBObject>() {

              @Override
              public DBObject apply(TupleEntry entry) {
                return transformer.transformEntry(entry);
              }

            })));

    return list;
  }

  private static Object extractPotentiallySensitiveObjectValue(
      ControlFieldsReference reference, FileType fileType, TupleEntry entry,
      String unprefixedFieldName, Redacting redacting) {

    return isSensitive(reference, fileType, unprefixedFieldName, redacting) ?

    LoaderModel.SENSITIVE_VALUE_REPLACEMENT : extractObjectValue(
        entry,
        prefixedFieldName(fileType, unprefixedFieldName));
  }

  private static Object extractObjectValue(TupleEntry entry, String prefixedLoaderFieldName) {
    return entry.getObject(new Fields(prefixedLoaderFieldName));
  }

  private static DBObject extractRawSequenceDataInfo(TupleEntry entry) {
    return transfromTupleEntries(
        getTuple(
            entry,
            AVAILABLE_RAW_SEQUENCE_DATA_FIELD),
        new RawSequenceDataInfo.DBObjectTransformer());
  }

  /**
   * All {@link FeatureType} are guaranteed to be present, whether provided or not in the submission, see
   * {@link DonorRecordLoaderFlowPlanner#addMissingTypeSummary()}.
   * <p>
   * TODO: make use of newly created {@link TupleEntryToDBObjectTransformer} interface.
   */
  private static DBObject extractSummary(TupleEntry entry) {
    BasicDBObjectBuilder builder = new BasicDBObjectBuilder();

    // Add summary information for each feature type
    for (FeatureType featureType : FeatureType.values()) {
      Fields summaryValueField = getSummaryValueField(featureType);
      builder.add(
          featureType.getSummaryFieldName(),
          featureType.isCountSummary() ? entry.getLong(summaryValueField) : entry.getBoolean(summaryValueField)); // Guaranteed
                                                                                                                  // present,
                                                                                                                  // see
                                                                                                                  // method
                                                                                                                  // comment
                                                                                                                  // above
    }

    // Add the available data types array
    builder.add(
        AVAILABLE_DATA_TYPES,
        extractAvailableDataTypes(entry));

    return builder.get();
  }

  /**
   * Extracts the available data types
   */
  private static DBObject extractAvailableDataTypes(@NonNull final TupleEntry entry) {
    val availableDataTypes = new BasicDBList();

    // Get the available data type tuple from the entry
    val tuple = getTuple(
        entry,
        getAvailableDataTypeField());

    // For each item of the tuple, get the type name as String and add it to the BasicDBList
    for (val object : tuple) {
      String typeName = (String) checkNotNull(object,
          "Expecting no null objects within %s", tuple);
      checkNotNull(FeatureType.from(typeName),
          "Expecting a feature type, instead got %s", typeName);
      availableDataTypes.add(typeName);
    }

    return availableDataTypes;
  }

  private boolean isNormalField(
      @NonNull final Collection<String> prefixedSubmissionFieldNames,
      @NonNull final String prefixedLoaderFieldName) {
    return prefixedSubmissionFieldNames
        .contains(prefixedLoaderFieldName);
  }

  private boolean isMetaField(
      @NonNull final Collection<String> prefixedSubmissionMetaFieldNames,
      @NonNull final String prefixedLoaderFieldName) {
    return prefixedSubmissionMetaFieldNames
        .contains(prefixedLoaderFieldName);
  }

  private static boolean isSensitive(
      @NonNull final ControlFieldsReference reference,
      @NonNull final FileType fileType,
      @NonNull final String unprefixedFieldName,
      @NonNull final Redacting redacting) {

    return redacting.isSensitiveFields() &&
        reference.isControlledField(fileType, unprefixedFieldName)
        && !Submission.isMutatedFromAlleleField(unprefixedFieldName);
  }

  private static boolean hasMetaType(@NonNull final HandlingType handlingType) {
    return handlingType.getDataType().isFeatureType()
        && handlingType.getSubType().isPrimarySubType();
  }

  private static Predicate<String> relevantFields(@NonNull final HandlingType handlingType) {
    return handlingType.getFileType()
        .isSsmP() ? (handlingType.getNesting().isObservation() ? observationFields() : occurenceFields()) : allFields();
  }

  private static boolean stillContainsValue(final Object value) {
    return value != LoaderModel.NO_ORIGINAL_VALUE
        && value != LoaderModel.SENSITIVE_VALUE_REPLACEMENT;
  }

  @Value
  public static class HandlingType {

    FileType fileType;
    Nesting nesting;

    public DataType getDataType() {
      return fileType.getDataType();
    }

    public FileSubType getSubType() {
      return fileType.getSubType();
    }

  }

  public static enum Nesting {

    /**
     * Result of joining 2 files.
     */
    JOIN,

    /**
     * No join involved, simply nesting some fields from the same file.
     */
    OBSERVATION;

    public boolean isObservation() {
      return this == OBSERVATION;
    }

    public boolean isJoin() {
      return this == JOIN;
    }
  }

  public static enum Redacting {

    /**
     * Redacting of sensitive fields (controlled fields except some specially handled ones).
     */
    SENSITIVE_FIELDS,

    /**
     * No redacting.
     */
    NONE;

    public boolean isSensitiveFields() {
      return this == SENSITIVE_FIELDS;
    }

    public boolean isNone() {
      return this == NONE;
    }

  }

}
