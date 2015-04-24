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
package org.icgc.dcc.etl.loader.service;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_LIBRARY_STRATEGY;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ASSEMBLY_VERSION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_STRAND;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_RAW_DATA_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_SEQUENCING_STRATEGY;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.CNSM_S_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SAMPLE_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SGV_P_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SGV_S_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SPECIMEN_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SSM_M_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SSM_P_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SSM_S_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.STSM_S_TYPE;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DONOR_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.generatedField;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.generatedFieldName;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.generatedFields;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.getGeneratedFieldName;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;
import static org.icgc.dcc.submission.dictionary.util.Dictionaries.readFileSchema;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.BusinessKeys;
import org.icgc.dcc.common.core.model.DataType;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames;
import org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames;
import org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames;
import org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames;
import org.icgc.dcc.common.core.model.FileTypes.FileSubType;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.common.core.model.ValueType;
import org.icgc.dcc.common.core.util.Strings2;
import org.icgc.dcc.submission.dictionary.model.Dictionary;
import org.icgc.dcc.submission.dictionary.model.Field;

import cascading.tuple.Fields;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Temporary class until the loader model (formerly known as "secondary dictionary") is fleshed out.
 */
@NoArgsConstructor(access = PRIVATE)
public class LoaderModel {

  /**
   * When no value was provided or a missing code was used for a given field.
   */
  public static final Object NO_ORIGINAL_VALUE = null;

  /**
   * Replacement value for a sensitive field.
   */
  public static final Object SENSITIVE_VALUE_REPLACEMENT = null;

  public static String getClinicalArrayInternalName(
      @NonNull final FileType fileType) {

    return getArrayInternalName(fileType);
  }

  public static boolean isClinicalArrayInternalName(
      @NonNull final FileType fileType,
      @NonNull final String fieldName) {

    return getClinicalArrayInternalName(fileType)
        .equals(fieldName);
  }

  private static String getArrayInternalName(
      @NonNull final FileType fileType) {

    return generatedFieldName(fileType);
  }

  public static Predicate<String> allFields() {
    return Predicates.<String> alwaysTrue();
  }

  /**
   * {@link Datatype}s whose {@link FileSubType#SECONDARY_SUBTYPE} require cloning of the genomic annotation.
   */
  public static final Collection<FeatureType> REQUIRES_ANNOTATION_INFO_CLONING = ImmutableSet.of(
      FeatureType.SSM_TYPE,
      FeatureType.SGV_TYPE,
      FeatureType.CNSM_TYPE);

  @NoArgsConstructor(access = PRIVATE)
  public static class Submission {

    private static final Collection<String> SSM_P_EXTRA_FIELD_NAMES = new ImmutableSet.Builder<String>()
        .add(NormalizerFieldNames.NORMALIZER_MARKING)
        .add(NormalizerFieldNames.NORMALIZER_MUTATION)
        .add(NormalizerFieldNames.NORMALIZER_OBSERVATION_ID)
        .build();

    private static final Collection<String> SGV_P_EXTRA_FIELD_NAMES = new ImmutableSet.Builder<String>()
        .add(NormalizerFieldNames.NORMALIZER_OBSERVATION_ID)
        .build();

    /**
     * Special field: controlled but masked where needed, hence not sensitive.
     */
    public static boolean isMutatedFromAlleleField(String fieldName) {
      return SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE.equals(fieldName);
    }

    private static Field createDummyField(String fieldName) {
      val field = new Field();
      field.setName(fieldName);
      field.setValueType(ValueType.TEXT);
      return field;
    }

    private static Set<Field> createExtraDummyFields(Collection<String> extraFieldNames) {
      val fields = new ImmutableSet.Builder<Field>();
      for (val fieldName : extraFieldNames) {
        fields.add(LoaderModel.Submission.createDummyField(fieldName));
      }
      return fields.build();
    }

    public static Dictionary augmentDictionary(Dictionary dictionary) {

      // Add fields to ssm_p
      val ssmP = dictionary.getFileSchema(SSM_P_TYPE);
      for (val field : LoaderModel.Submission.createExtraDummyFields(SSM_P_EXTRA_FIELD_NAMES)) {
        ssmP.addField(field);
      }

      // Add fields to sgv_p
      val sgvP = dictionary.getFileSchema(SGV_P_TYPE);
      for (val field : LoaderModel.Submission.createExtraDummyFields(SGV_P_EXTRA_FIELD_NAMES)) {
        sgvP.addField(field);
      }

      // Add file schemata
      dictionary.addFile(readFileSchema(SSM_S_TYPE));
      dictionary.addFile(readFileSchema(SGV_S_TYPE));

      return dictionary;
    }

  }

  @NoArgsConstructor(access = PRIVATE)
  public static class Consequence {

    public static String getConsequenceArrayInternalName(
        @NonNull final FileType fileType) {

      return getArrayInternalName(fileType);
    }

    private static boolean isConsequenceArrayInternalName(
        @NonNull final FileType fileType,
        @NonNull final String fieldName) {

      return getConsequenceArrayInternalName(fileType)
          .equals(fieldName);
    }

  }

  @NoArgsConstructor(access = PRIVATE)
  public static class RawSequence {

    /**
     * Mapping for resulting mongo fields.
     */
    public static Map<String, String> RAW_SEQUENCE_DATA_FIELDS_MAPPING = ImmutableMap.of(
        SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION, SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION,
        SUBMISSION_OBSERVATION_RAW_DATA_REPOSITORY, SEQUENCE_DATA_REPOSITORY,
        SUBMISSION_OBSERVATION_SEQUENCING_STRATEGY, SEQUENCE_DATA_LIBRARY_STRATEGY);

    /**
     * Value used to replace any null or empty {@link String}s.
     */
    public static final String MISSING_VALUE_REPLACEMENT = "N/A";

    public static Fields AVAILABLE_RAW_SEQUENCE_DATA_FIELD =
        generatedFields(LoaderFieldNames.AVAILABLE_RAW_SEQUENCE_DATA);

    // Join field
    public static Fields META_FILES_SAMPLE_ID_FIELD = new Fields(SUBMISSION_OBSERVATION_ANALYZED_SAMPLE_ID);

    // Retain (original meta file)
    public static Fields META_FILE_RETAIN_FIELDS =
        META_FILES_SAMPLE_ID_FIELD
            .append(
            new Fields(
                SUBMISSION_OBSERVATION_SEQUENCING_STRATEGY,
                SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION,
                SUBMISSION_OBSERVATION_RAW_DATA_REPOSITORY));

    public static boolean isAvailableRawSequenceDataField(
        @NonNull final String prefixedFieldName) {

      return generatedFieldName(LoaderFieldNames.AVAILABLE_RAW_SEQUENCE_DATA)
          .equals(prefixedFieldName);
    }

  }

  @NoArgsConstructor(access = PRIVATE)
  public static class Summary {

    public static final Fields SUMMARY_RESULT_FIELD = generatedFields(LoaderFieldNames.SUMMARY);

    public static boolean isSummaryField(
        @NonNull final String prefixedFieldName) {

      return generatedFieldName(LoaderFieldNames.SUMMARY)
          .equals(prefixedFieldName);
    }

  }

  @NoArgsConstructor(access = PRIVATE)
  public static class Occurence {

    public static final String NO_VALUE_REPLACEMENT = Strings2.EMPTY_STRING;
    public static final FileType OBSERVATION_FILE_TYPE = getObservationFileType();

    public static final Fields CONSEQUENCE_FIELD = generatedField(SSM_S_TYPE);
    public static final Fields OBSERVATION_ARRAY_FIELD = generatedField(OBSERVATION_FILE_TYPE);

    public static final Fields OCCURENCE_GROUP_BY_FIELDS =
        generatedFields(IdentifierFieldNames.SURROGATE_DONOR_ID)
            .append(generatedFields(IdentifierFieldNames.SURROGATE_MUTATION_ID));

    public static final Fields SECONDARY_SORT_FIELDS =
        prefixedFields(
            OBSERVATION_FILE_TYPE, SubmissionFieldNames.SUBMISSION_OBSERVATION_ANALYSIS_ID)
            .append(prefixedFields(
                OBSERVATION_FILE_TYPE, SubmissionFieldNames.SUBMISSION_OBSERVATION_ANALYZED_SAMPLE_ID));

    /**
     * Original from the standpoint of the loader.
     */
    public static final Collection<String> SSM_OCCURENCE_ORIGINAL_FIELD_NAMES = new ImmutableSet.Builder<String>()
        .addAll(BusinessKeys.MUTATION_IDENTIFYING_PART)
        .addAll(ImmutableSet.of(

            // Since business key uses the combined "mutation" version
            SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE,
            SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE,

            // Redundant fields
            SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE,
            SUBMISSION_OBSERVATION_CHROMOSOME_STRAND))
        .build();

    public static final Collection<String> SSM_OCCURRENCE_FIELD_NAMES = new ImmutableSet.Builder<String>()
        .addAll(SSM_OCCURENCE_ORIGINAL_FIELD_NAMES)
        .build();

    private static final Iterable<Fields> OCCURENCE_SUBMISSION_FIELDS = ImmutableSet.copyOf(
        transform(
            SSM_OCCURENCE_ORIGINAL_FIELD_NAMES,
            new Function<String, Fields>() {

              @Override
              public Fields apply(String fieldName) {
                return prefixedFields(
                    isFromMeta(fieldName) ?
                        SSM_M_TYPE :
                        SSM_P_TYPE,
                    fieldName);
              }

              private boolean isFromMeta(String fieldName) {
                return SUBMISSION_OBSERVATION_ASSEMBLY_VERSION.equals(fieldName);
              }

            })
        );

    public static final Fields OCCURENCE_FIELDS =
        OCCURENCE_GROUP_BY_FIELDS
            .append(newArrayList(OCCURENCE_SUBMISSION_FIELDS).toArray(new Fields[] {}))
            .append(CONSEQUENCE_FIELD);

    public static FileType getObservationFileType() {
      return SSM_P_TYPE;
    }

    public static Predicate<String> occurenceFields() {
      return Predicates.in(SSM_OCCURRENCE_FIELD_NAMES);
    }

    public static Predicate<String> observationFields() {
      return not(occurenceFields());
    }

    public static boolean isObservationArrayInternalName(
        @NonNull final String fieldName) {

      return getObservationArrayInternalName()
          .equals(fieldName);
    }

    public static String getObservationArrayInternalName() {
      return getArrayInternalName(getObservationFileType());
    }

  }

  public static ReleaseCollection getCollection(
      @NonNull final DataType dataType) {

    return dataType.isClinicalType() ?
        DONOR_COLLECTION :
        OBSERVATION_COLLECTION;
  }

  @NoArgsConstructor(access = PRIVATE)
  public static class Persistence {

    // TODO: generate
    public static final Collection<String> JOIN_ARRAY_INTERNAL_NAMES = ImmutableSet.of(
        Consequence.getConsequenceArrayInternalName(SPECIMEN_TYPE),
        Consequence.getConsequenceArrayInternalName(SAMPLE_TYPE),
        Consequence.getConsequenceArrayInternalName(SSM_S_TYPE),
        Consequence.getConsequenceArrayInternalName(CNSM_S_TYPE),
        Consequence.getConsequenceArrayInternalName(STSM_S_TYPE),
        Consequence.getConsequenceArrayInternalName(SGV_S_TYPE));

    public static final Collection<String> GENERATED_FIELD_NAMES = ImmutableSet.of(
        IdentifierFieldNames.SURROGATE_DONOR_ID,
        IdentifierFieldNames.SURROGATE_SPECIMEN_ID,
        IdentifierFieldNames.SURROGATE_SAMPLE_ID,
        IdentifierFieldNames.SURROGATE_MUTATION_ID,
        LoaderFieldNames.OBSERVATION_TYPE,
        LoaderFieldNames.PROJECT_ID,
        LoaderFieldNames.GENE_ID,
        LoaderFieldNames.TRANSCRIPT_ID,
        LoaderFieldNames.SURROGATE_MATCHED_SAMPLE_ID,
        LoaderFieldNames.AVAILABLE_RAW_SEQUENCE_DATA,
        LoaderFieldNames.SUMMARY);

    public static final Collection<String> SUPPLEMENTAL_FIELD_NAMES = ImmutableSet.of(
        "supplemental$donor_id",
        "supplemental$rest");

    public static boolean isGeneratedField(
        @NonNull final String fieldName) {

      return newArrayList(transform(
          Persistence.GENERATED_FIELD_NAMES,
          getGeneratedFieldName()))
          .contains(fieldName);
    }

    public static boolean isSupplementalField(
        @NonNull final String fieldName) {

      return SUPPLEMENTAL_FIELD_NAMES.contains(fieldName);
    }

    public static boolean isJoinArrayInternalName(
        @NonNull final String fieldName) {

      return JOIN_ARRAY_INTERNAL_NAMES
          .contains(fieldName);
    }

    public static Iterable<FileType> getNestedTypes(
        @NonNull final FileType fileType) {

      // TODO: infer from model
      switch (fileType) {
      case DONOR_TYPE:
        return newArrayList(SPECIMEN_TYPE);
      case SPECIMEN_TYPE:
        return newArrayList(SAMPLE_TYPE);
      case SSM_P_TYPE:
        return newArrayList(SSM_S_TYPE);
      case CNSM_P_TYPE:
        return newArrayList(CNSM_S_TYPE);
      case STSM_P_TYPE:
        return newArrayList(STSM_S_TYPE);
      case SGV_P_TYPE:
        return newArrayList(SGV_S_TYPE);
      default:
        return newArrayList();
      }
    }

    public static FileType getNestedFileType(
        @NonNull final FileType fileType,
        @NonNull final String fieldName) {
      checkArgument(JOIN_ARRAY_INTERNAL_NAMES.contains(fieldName), fieldName);

      if (Occurence.isObservationArrayInternalName(fieldName)) {
        return Occurence.getObservationFileType();
      } else {
        val nestedFileType = getNestedFileType(fileType);
        if (isClinicalArrayInternalName(nestedFileType, fieldName) ||
            Consequence.isConsequenceArrayInternalName(nestedFileType, fieldName)) {
          return nestedFileType;
        } else {
          throw new IllegalStateException(fieldName + ", " + fileType);
        }
      }
    }

    public static String potentiallyRenameArray(@NonNull final FileType fileType) {

      switch (fileType) {
      case SPECIMEN_TYPE:
        // Fall through
      case SAMPLE_TYPE:
        return getClinicalArrayExternalName(fileType);
      case SSM_P_TYPE:
        checkState(fileType == Occurence.getObservationFileType()); // By design
        return getObservationArrayExternalName();
      default:
        checkState(fileType.isSecondary());
        return getConsequenceArrayExternalName();
      }
    }

    private static FileType getNestedFileType(@NonNull final FileType fileType) {

      // TODO: improve
      if (fileType.isDonor()) {
        return SPECIMEN_TYPE;
      } else if (fileType.isSpecimen()) {
        return SAMPLE_TYPE;
      }

      checkState(fileType.getDataType().isFeatureType());

      val optional = fileType.getDataType().asFeatureType().getSecondaryFileType();
      checkArgument(optional.isPresent());

      return optional.get();
    }

    private static String getClinicalArrayExternalName(
        @NonNull final FileType fileType) {

      return getArrayExternalName(fileType);
    }

    private static String getConsequenceArrayExternalName() {
      return LoaderFieldNames.CONSEQUENCE_ARRAY_NAME;
    }

    private static String getObservationArrayExternalName() {
      return LoaderFieldNames.OBSERVATION_ARRAY_NAME;
    }

    private static String getArrayExternalName(FileType fileType) {
      return fileType.getId();
    }

  }

}
