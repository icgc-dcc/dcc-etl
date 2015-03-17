/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FileTypes.FileSubType.META_SUBTYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SAMPLE_TYPE;
import static org.icgc.dcc.common.core.model.SpecialValue.FULL_MISSING_CODES;
import static org.icgc.dcc.common.core.model.ValueType.TEXT;
import static org.icgc.dcc.common.core.util.Strings2.EMPTY_STRING;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.collect.SerializableMaps;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.model.SubmissionModel;
import org.icgc.dcc.common.core.model.SubmissionModel.FileModel;
import org.icgc.dcc.common.core.model.SubmissionModel.FileModel.FieldModel;
import org.icgc.dcc.common.core.model.SubmissionModel.JoinModel;
import org.icgc.dcc.common.core.model.ValueType;
import org.icgc.dcc.common.core.util.Functions2;
import org.icgc.dcc.common.core.util.Optionals;
import org.icgc.dcc.submission.dictionary.model.CodeList;
import org.icgc.dcc.submission.dictionary.model.Dictionary;
import org.icgc.dcc.submission.dictionary.model.Field;
import org.icgc.dcc.submission.dictionary.model.FileSchema;
import org.icgc.dcc.submission.dictionary.model.Relation;
import org.icgc.dcc.submission.dictionary.model.Restriction;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

@NoArgsConstructor(access = PRIVATE)
public class SubmissionModelFactory {

  private static final String CODELIST_NAME_PROPERTY = "name";

  public static SubmissionModel get(
      @NonNull final Dictionary dictionary,
      @NonNull final List<CodeList> codeLists) {

    val relevantFileSchemata = getRelevantFileSchemata(dictionary);

    return new SubmissionModel(
        dictionary.getVersion(),
        getFiles(relevantFileSchemata, codeLists),
        getJoins(relevantFileSchemata),
        getPks(relevantFileSchemata),
        getFks(relevantFileSchemata),
        getGeneralMapping());
  }

  private static Map<FileType, FileModel> getFiles(
      @NonNull final Iterable<FileSchema> fileSchemata,
      @NonNull final List<CodeList> codeLists) {

    val builder = new ImmutableMap.Builder<FileType, FileModel>();
    for (val fileSchema : fileSchemata) {
      builder.put(
          fileSchema.getFileType(),
          new FileModel(
              Pattern.compile(fileSchema.getPattern()),
              getFields(
                  fileSchema,
                  codeLists)));
    }

    return builder.build();
  }

  private static Map<String, FieldModel> getFields(
      @NonNull final FileSchema fileSchema,
      @NonNull final List<CodeList> codeLists) {

    val builder = new ImmutableMap.Builder<String, FieldModel>();
    for (val field : fileSchema.getFields()) {
      builder.put(
          field.getName(),
          new FieldModel(
              getValueType(field),
              field.isControlled(),
              getMapping(
                  codeLists,
                  field.getCodeListRestriction())));
    }

    return builder.build();
  }

  private static Map<FileType, JoinModel> getJoins(
      @NonNull final Iterable<FileSchema> fileSchemata) {

    val builder = new ImmutableMap.Builder<FileType, JoinModel>();
    for (val fileSchema : fileSchemata) {
      for (val relation : getRelevantRelations(fileSchema)) {
        builder.put(
            fileSchema.getFileType(),
            new JoinModel(
                relation.getOtherFileType(),
                relation.isBidirectional()));
      }
    }

    return builder.build();
  }

  private static Map<FileType, List<String>> getPks(@NonNull final Iterable<FileSchema> fileSchemata) {

    val builder = new ImmutableMap.Builder<FileType, List<String>>();

    // Several relations point to SAMPLE, but we only care to gather the PK once
    boolean firstSampleSchema = true;

    for (val fileSchema : fileSchemata) {
      for (val relation : getRelevantRelations(fileSchema)) {
        val otherFileType = relation.getOtherFileType();
        if (!otherFileType.isSample() || firstSampleSchema) {
          builder.put(
              otherFileType,
              copyOf(relation.getOtherFields()));

        }

        if (otherFileType.isSample()) {

          // Ensure consistent with first sample schema
          checkState(relation.getOtherFields().equals(builder.build().get(SAMPLE_TYPE)));

          firstSampleSchema = false;
        }
      }
    }

    return builder.build();
  }

  private static Map<FileType, List<String>> getFks(@NonNull final Iterable<FileSchema> fileSchemata) {

    val builder = new ImmutableMap.Builder<FileType, List<String>>();
    for (val fileSchema : fileSchemata) {
      for (val relation : getRelevantRelations(fileSchema)) {
        builder.put(
            fileSchema.getFileType(), // TODO: conflicts
            copyOf(relation.getFields()));
      }
    }
    return builder.build();
  }

  /**
   * Some file schemata are not relevant to the ETL.
   */
  private static Iterable<FileSchema> getRelevantFileSchemata(@NonNull final Dictionary dictionary) {

    return filter(
        dictionary.getFiles(),
        new Predicate<FileSchema>() {

          @Override
          public boolean apply(FileSchema fileSchema) {
            return !fileSchema.getFileType().isOptional();
          }

        });
  }

  /**
   * Some relations are not relevant to the ETL.
   */
  private static Iterable<Relation> getRelevantRelations(@NonNull final FileSchema fileSchema) {

    return filter(
        fileSchema.getRelations(),
        new Predicate<Relation>() {

          @Override
          public boolean apply(Relation relation) {
            return !isSystemFileType(relation)
                && (!isMetaFileType(fileSchema) || !pertainsTo(relation, SUBMISSION_MATCHED_SAMPLE_ID));
          }

          private boolean isSystemFileType(Relation relation) {
            return FileType.METH_ARRAY_PROBES_TYPE == relation.getOtherFileType();
          }

          private boolean isMetaFileType(final FileSchema fileSchema) {
            return fileSchema.getFileType().getSubType() == META_SUBTYPE;
          }

          private boolean pertainsTo(Relation relation, String fieldName) {
            return relation.getFields().contains(fieldName);
          }

        });
  }

  private static Optional<Map<String, String>> getMapping(
      @NonNull final List<CodeList> codeLists,
      @NonNull final Optional<Restriction> codeListRestriction) {

    return codeListRestriction.isPresent() ?
        Optional.of(
            find(codeLists,

                new Predicate<CodeList>() {

                  final String codeListName = getCodeListName(codeListRestriction.get());

                  @Override
                  public boolean apply(CodeList codeList) {
                    return codeListName.equals(codeList.getName());
                  }

                })
                .asMap()) :
        Optionals.ABSENT_STRING_MAP;
  }

  private static Map<String, String> getGeneralMapping() {

    return SerializableMaps.asMap(
        newLinkedHashSet(FULL_MISSING_CODES),
        Functions2.<String, String> constant(EMPTY_STRING));
  }

  private static String getCodeListName(Restriction codeListRestriction) {
    return (String) codeListRestriction.getConfig().get(CODELIST_NAME_PROPERTY);
  }

  private static ValueType getValueType(Field field) {
    return ignoreOriginalValueType(field) ? TEXT : field.getValueType();
  }

  private static boolean ignoreOriginalValueType(Field field) {
    return field.hasCodeListRestriction() || field.hasInRestriction();
  }

}
