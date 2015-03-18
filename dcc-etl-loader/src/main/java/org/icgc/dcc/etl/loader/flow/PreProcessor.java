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
package org.icgc.dcc.etl.loader.flow;

import static cascading.tuple.Fields.ARGS;
import static cascading.tuple.Fields.REPLACE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static org.icgc.dcc.common.core.model.SpecialValue.NO_VALUE;
import static org.icgc.dcc.common.core.model.SpecialValue.isDeprecatedValue;
import static org.icgc.dcc.common.core.model.SpecialValue.isFullMissingCode;
import static org.icgc.dcc.common.core.util.Joiners.INDENT;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;
import static org.icgc.dcc.common.cascading.Fields2.cloneFields;
import static org.icgc.dcc.common.cascading.Fields2.fields;
import static org.icgc.dcc.common.cascading.Fields2.getFieldNames;

import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.model.ValueType;
import org.icgc.dcc.common.cascading.RemoveHollowTupleFilter;
import org.icgc.dcc.common.cascading.operation.BaseFunction;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.OperationException;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Rename;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;

import com.google.common.base.Optional;

/**
 * Must be completely rewritten (DCC-994).
 */
@Slf4j
public class PreProcessor extends SubAssembly {

  public PreProcessor(
      @NonNull final Pipe pipe,
      @NonNull final FileType fileType,
      @NonNull final List<String> fieldNames,
      @NonNull final Fields header,
      @NonNull final Map<String, ValueType> valueTypes,
      @NonNull final Map<String, Optional<Map<String, String>>> fileCodeList) {
    setTails(applyDefaultPipes(pipe, fileType, fieldNames, header, valueTypes, fileCodeList));
  }

  /**
   * Operations that must happen on any input file for the loader.
   */
  private Pipe applyDefaultPipes(
      @NonNull final Pipe pipe,
      @NonNull final FileType fileType,
      @NonNull final List<String> fieldNames,
      @NonNull final Fields header,
      @NonNull final Map<String, ValueType> valueTypes,
      @NonNull final Map<String, Optional<Map<String, String>>> fileCodeList) {

    return renamePipe( // Prefix all fields with current schema
        preProcessEach(
            new Each(
                pipe,
                new RemoveHollowTupleFilter()),
            fileType,
            fieldNames,
            header,
            valueTypes,
            fileCodeList),
        fileType,
        fieldNames);
  }

  /**
   * In the future PreProcessFunction will be split into several operations (DCC-994) - wip
   */
  private Pipe preProcessEach(
      @NonNull final Pipe pipe,
      @NonNull final FileType fileType,
      @NonNull final List<String> fieldNames,
      @NonNull final Fields header,
      @NonNull final Map<String, ValueType> valueTypes,
      @NonNull final Map<String, Optional<Map<String, String>>> fileCodeList) {

    val incomingFields = getIncomingFields(
        fileType,
        fieldNames,
        header);

    return new Each(
        pipe,
        incomingFields,
        new PreProcessFunction(
            incomingFields,
            fileType,
            valueTypes,
            fileCodeList),
        REPLACE);
  }

  private Fields getIncomingFields(
      @NonNull final FileType fileType,
      @NonNull final Iterable<String> fieldNames,
      @NonNull final Fields headerFields) {
    Fields expectedFields = fields(fieldNames);
    Fields incomingFields = cloneFields(headerFields);

    // We expect the fields in order now, with no missing/extra ones
    checkState(expectedFields.equals(incomingFields), "'%s': '\n%s\n' != '\n%s\n'",
        fileType,
        INDENT.join(getFieldNames(expectedFields)),
        INDENT.join(getFieldNames(incomingFields)));

    return incomingFields;
  }

  private Pipe renamePipe(
      @NonNull final Pipe pipe,
      @NonNull final FileType fileType,
      @NonNull final Iterable<String> fieldNames) {
    return new Rename(
        pipe,
        fields(fieldNames),
        prefixedFields(
            fileType.getId(),
            fieldNames));
  }

  private class PreProcessFunction extends BaseFunction<Void> {

    /**
     * The {@link Fields} in the order they appear in the submission file, minus any extra fields.
     */
    private final Fields incomingFields;
    private final FileType fileType;
    private final Map<String, ValueType> fileSchemaFieldsMap;
    private final Map<String, Optional<Map<String, String>>> codeLists;

    /**
     * TODO: Address trick to know what the header contain: DCC-996 (also in PreProcessFunction)
     */
    public PreProcessFunction(
        @NonNull final Fields incomingFields,
        @NonNull final FileType fileType,
        @NonNull final Map<String, ValueType> fields,
        @NonNull final Map<String, Optional<Map<String, String>>> codeLists) {
      super(ARGS);

      this.fileType = fileType;
      this.fileSchemaFieldsMap = checkNotNull(fields);
      this.codeLists = codeLists;
      this.incomingFields = checkNotNull(incomingFields);
    }

    @Override
    public void operate(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        FunctionCall<Void> functionCall) {
      TupleEntry entry = functionCall.getArguments();

      Tuple newTuple = new Tuple(entry.getTuple());

      // TODO: should be different operations - started to split it already, see in CascadingFunctions
      trimValues(newTuple);
      clearMissingValues(newTuple);
      translateCodes(newTuple);
      coerceTypes(newTuple);

      functionCall.getOutputCollector().add(newTuple);
    }

    /**
     * FIXME: Neither operations belong to the loader (see https://jira.oicr.on.ca/browse/DCC-1641 and DCC-1941 and
     * DCC-1942)
     */
    private void trimValues(Tuple newTuple) {
      for (int i = 0; i < newTuple.size(); i++) {
        String value = newTuple.getString(i);
        if (value != null) {
          value = value.trim();
        }
        newTuple.set(i, value);
      }
    }

    private void clearMissingValues(Tuple newTuple) {
      for (int i = 0; i < newTuple.size(); i++) {
        String value = newTuple.getString(i);
        if (value != null) {

          // Replace any empty value, missing code or former missing code with null
          if (value.isEmpty() || isFullMissingCode(value) || isDeprecatedValue(value)) {
            // TODO: add a check that this really should be read as a missing code?
            newTuple.set(i, NO_VALUE);
          }
        }
      }
    }

    private void translateCodes(Tuple newTuple) {
      int index = 0;
      for (String fieldName : getFieldNames(incomingFields)) {
        Optional<Map<String, String>> codeList = codeLists.get(fieldName);
        if (codeList.isPresent()) {
          String originalValue = // Either term.code or term.value (see DCC-904's first comment)
              newTuple.getString(index);
          if (originalValue != null) { // Else value was cleared, translation not required

            // Translate term code to value
            val mapping = codeList.get();
            if (mapping.containsKey(originalValue)) {
              newTuple.setString(index, mapping.get(originalValue));
            } else {
              if (!mapping.containsValue(originalValue)) {
                log.error(
                    "Invalid value found, neither null, a term code or a term value: '{}' (field '{}' in file schema '{}')",
                    new Object[] { originalValue, fieldName, fileType });
              }
            }
          }
        }

        index++;
      }
    }

    private Tuple coerceTypes(Tuple newTuple) {
      checkState(incomingFields.size() <= fileSchemaFieldsMap.size(), "Unexpected fields: %s > %s, (%s and %s)",
          incomingFields.size(), fileSchemaFieldsMap.size(), incomingFields, fileSchemaFieldsMap.keySet());
      Class<?>[] types = types(incomingFields);

      try {
        Tuples.coerce(newTuple, types, newTuple);
      } catch (OperationException e) { // so as to complete cascading's message with something more informative...
        log.error("Failed to coerce tuple {} using types {}", newTuple, asList(types));
        throw e;
      } catch (IllegalArgumentException e) { // FIXME: not sure why this doesn't seem to work, even though
                                             // NumberFormatException for instance is an IllegalArgumentException...
        log.error(
            "could not coerce tuple [{}] using {}; header is [{}]; fileType: {}, fields: {}",
            new Object[] { newTuple, asList(types), incomingFields, fileType, fileSchemaFieldsMap });
        throw e;
      }
      return newTuple;
    }

    private Class<?>[] types(Fields sourceFields) {
      Class<?>[] types = new Class[sourceFields.size()];
      int i = 0;
      for (String fieldName : getFieldNames(sourceFields)) {
        ValueType type = fileSchemaFieldsMap.get(fieldName);
        checkNotNull(type, "Could not find %s within %s", fieldName, fileSchemaFieldsMap.keySet());

        log.debug(fileType + "\t" + fieldName + "\t" + type);
        types[i++] = type.getJavaType();
      }
      return types;
    }

  }

}
