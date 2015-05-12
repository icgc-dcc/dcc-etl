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
package org.icgc.dcc.etl.dictionary;

import static com.google.common.base.Joiner.on;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newTreeMap;
import static java.util.Collections.sort;
import static org.apache.commons.lang.StringUtils.repeat;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import javax.validation.constraints.NotNull;

import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;

import org.icgc.dcc.common.core.model.ValueType;
import org.icgc.dcc.submission.dictionary.model.Dictionary;
import org.icgc.dcc.submission.dictionary.model.Field;
import org.icgc.dcc.submission.dictionary.model.FileSchema;
import org.icgc.dcc.submission.dictionary.model.Relation;
import org.icgc.dcc.submission.dictionary.model.Restriction;
import org.icgc.dcc.submission.dictionary.model.RestrictionType;
import org.icgc.dcc.submission.dictionary.model.SummaryType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ComparisonChain;
import com.mongodb.BasicDBObject;

/**
 * Parses dictionary and reports on values requiring particular attention (until https://jira.oicr.on.ca/browse/DCC-904
 * is done). TODO: rewrite in python or design proper component to get views on dictionary?
 * <p>
 * Typical argument: ../../dcc-submission/dcc-submission-server/src/main/resources/0.6c.CLOSED.json
 * <p>
 * Very rudimentary for now.
 * <p>
 * TODO: Add reporting of fields on which a regex is defined.
 */
public class Main {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String FIELD_SEPARATOR = "\t";

  private static final List<String> CLINICAL_FILE_SCHEMA_NAMES =
      newArrayList("donor", "specimen", "sample");

  private static final List<String> OPTIONAL_FILE_SCHEMA_NAMES =
      newArrayList("biomarker", "exposure", "family", "surgery", "therapy");

  private static final List<String> RELATIONS_IGNORE_LIST = newArrayList("hsap_gene");

  private static final List<String> UNIQUE_FIELD_IGNORE_LIST = newArrayList("variant_id");

  private static final List<String> FIELDS_IGNORE_LIST = newArrayList("sample_id");

  private static final Map<String, Set<String>> UNIQUE_MAP = newTreeMap();

  private static final Map<String, Set<String>> RELATION_REQUIRED_MAP = newTreeMap();

  private static final String CODELIST_NAME_RESTRICTION_PARAMETER = "name";
  private static final String ACCEPT_MISSING_CODE_RESTRICTION_PARAMETER = "acceptMissingCode";

  @SneakyThrows
  public static void main(String[] args) {
    String dictionaryPath = args[0];
    parse(dictionaryPath);
  }

  private static void parse(String dictionaryPath) throws IOException, JsonParseException, JsonMappingException {
    Dictionary dictionary = MAPPER.readValue(new File(dictionaryPath), Dictionary.class);

    initialize(dictionary);

    reportDictionaryFields(dictionary);
    reportSummaryTypes(dictionary);
    reportRestrictionsCompatibility(dictionary);

    populateUniqueness(dictionary);
    populateRelationRequired(dictionary);

    reportFieldsRequiredness(dictionary, false);
    reportFieldsRequiredness(dictionary, true);

    reportReferencedFieldsUniqueness(dictionary);
    reportRelationsSurjectivity(dictionary);

    reportUniqueFieldsRequiredness(dictionary);
    reportRelationFieldsRequiredness(dictionary);

    reportCodelists(dictionary);
    reportValueTypes(dictionary);

    createDotVersion(dictionary);
  }

  private static void initialize(Dictionary dictionary) {
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      UNIQUE_MAP.put(fileSchema.getName(), new TreeSet<String>());
      RELATION_REQUIRED_MAP.put(fileSchema.getName(), new TreeSet<String>());
    }
  }

  private static void populateUniqueness(Dictionary dictionary) {
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      String fileSchemaName = fileSchema.getName();
      List<String> uniqueFields = fileSchema.getUniqueFields();
      if (uniqueFields != null) {
        UNIQUE_MAP.get(fileSchemaName).addAll(uniqueFields);
      }
    }
  }

  private static void populateRelationRequired(Dictionary dictionary) {
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      String fileSchemaName = fileSchema.getName();

      for (Relation relation : fileSchema.getRelations()) {
        String other = relation.getOther();
        List<String> otherFields = relation.getOtherFields();

        if (RELATIONS_IGNORE_LIST.contains(other) == false) {
          RELATION_REQUIRED_MAP.get(fileSchemaName).addAll(relation.getFields());
          RELATION_REQUIRED_MAP.get(other).addAll(otherFields);
        }
      }
    }
  }

  private static void reportDictionaryFields(Dictionary dictionary) {
    describe("Dictionary fields");

    Map<String, List<String>> map = newTreeMap();
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      String fileSchemaName = fileSchema.getName();

      List<String> list = newArrayList();
      for (Field field : fileSchema.getFields()) {
        list.add(field.getName());
      }
      sort(list);
      map.put(fileSchemaName, list);
    }

    for (Entry<String, List<String>> entry : map.entrySet()) {
      System.out.println(on(FIELD_SEPARATOR).join(entry.getKey(), entry.getValue()));
    }
  }

  private static void reportSummaryTypes(Dictionary dictionary) {
    describe("Summary types");

    Map<String, List<String>> map = newTreeMap();
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      String fileSchemaName = fileSchema.getName();

      for (Field field : fileSchema.getFields()) {

        SummaryType summaryType = field.getSummaryType();
        if (SummaryType.MIN_MAX == summaryType || SummaryType.AVERAGE == summaryType) {
          String fieldName = field.getName();
          ValueType valueType = field.getValueType();
          List<String> sublist = newArrayList(fileSchemaName, fieldName, valueType.toString(), summaryType.toString());
          map.put(sublist.toString(), sublist);
        }
      }
    }

    for (Entry<String, List<String>> entry : map.entrySet()) {
      System.out.println(on(FIELD_SEPARATOR).join(entry.getValue()));
    }
  }

  private static void reportRestrictionsCompatibility(Dictionary dictionary) {
    describe("Restrictions compatibility");

    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      for (Field field : fileSchema.getFields()) {
        checkState(!(field.hasRegexRestriction() && field.hasCodeListRestriction()));
      }
    }
    System.out.println("OK");
  }

  private static void reportFieldsRequiredness(Dictionary dictionary, boolean strict) {
    describe("Fields " + (strict ? "strictly" : "loosely") + " requiredness");

    List<FieldsRequiredness> list = newArrayList();
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      String fileSchemaName = fileSchema.getName();
      List<String> requiredFields = newArrayList();

      for (Field field : fileSchema.getFields()) {
        Optional<Restriction> optional = field.getRestriction(RestrictionType.REQUIRED);
        if (optional.isPresent()) {
          Restriction requiredRestriction = optional.get();
          if ((strict && isStrict(requiredRestriction)) || (!strict && !isStrict(requiredRestriction))) {
            requiredFields.add(field.getName());
          }
        }
      }

      list.add(new FieldsRequiredness(fileSchemaName, requiredFields));
    }

    sort(list);
    for (FieldsRequiredness item : list) {
      System.out.println(item);
    }
  }

  private static void reportReferencedFieldsUniqueness(Dictionary dictionary) {
    describe("Referenced fields' uniqueness");

    List<ReferencedFieldsUniqueness> list = newArrayList();
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {

      for (Relation relation : fileSchema.getRelations()) {
        String other = relation.getOther();
        List<String> otherFields = relation.getOtherFields();

        if (RELATIONS_IGNORE_LIST.contains(other) == false) {
          boolean isUnique = UNIQUE_MAP.get(other).containsAll(otherFields);
          list.add(new ReferencedFieldsUniqueness(other, otherFields, isUnique));
        }
      }
    }

    sort(list);
    for (ReferencedFieldsUniqueness item : list) {
      System.out.println(item);
    }
  }

  private static void reportRelationsSurjectivity(Dictionary dictionary) {
    describe("Relations surjectivity");

    List<RelationsSurjectivity> list = newArrayList();
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      String fileSchemaName = fileSchema.getName();

      for (Relation relation : fileSchema.getRelations()) {
        String other = relation.getOther();
        boolean bidirectional = relation.isBidirectional();

        if (RELATIONS_IGNORE_LIST.contains(other) == false) {
          list.add(new RelationsSurjectivity(fileSchemaName, other, bidirectional));
        }
      }
    }

    sort(list);
    for (RelationsSurjectivity item : list) {
      System.out.println(item);
    }
  }

  private static void reportUniqueFieldsRequiredness(Dictionary dictionary) {
    describe("Unique fields' requiredness");

    List<SchemaUniqueFieldsRequiredness> list = newArrayList();
    for (Entry<String, Set<String>> entry : UNIQUE_MAP.entrySet()) {
      String fileSchemaName = entry.getKey();

      List<UniqueFieldsRequiredness> sublist = newArrayList();
      for (String fieldName : entry.getValue()) {
        if (UNIQUE_FIELD_IGNORE_LIST.contains(fieldName) == false) {
          boolean strictlyRequired = isStrictlyRequired(dictionary, fileSchemaName, fieldName);
          sublist.add(new UniqueFieldsRequiredness(fieldName, strictlyRequired));
        }
      }

      list.add(new SchemaUniqueFieldsRequiredness(fileSchemaName, sublist));
    }

    sort(list);
    for (SchemaUniqueFieldsRequiredness item : list) {
      System.out.println(item);
    }
  }

  private static void reportRelationFieldsRequiredness(Dictionary dictionary) {
    describe("Relation fields' requiredness");

    for (Entry<String, Set<String>> entry : RELATION_REQUIRED_MAP.entrySet()) {
      String fileSchemaName = entry.getKey();
      System.out.println(fileSchemaName);

      for (String fieldName : entry.getValue()) {
        if (FIELDS_IGNORE_LIST.contains(fieldName) == false) {
          boolean strictlyRequired = isStrictlyRequired(dictionary, fileSchemaName, fieldName);
          System.out.println("\t" + fieldName + "\t" + strictlyRequired);
        }
      }
    }
  }

  private static void reportCodelists(Dictionary dictionary) {
    reportFields(dictionary, true);
  }

  private static void reportValueTypes(Dictionary dictionary) {
    reportFields(dictionary, false);
  }

  private static void reportFields(Dictionary dictionary,
      boolean codelists) { // TODO: enum instead of boolean
    describe(codelists ? "Codelists" : "Value types (non-TEXT)");

    Map<String, List<Entry<String, String>>> map = newTreeMap();
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      String fileSchemaName = fileSchema.getName();

      List<Entry<String, String>> list = newArrayList();
      for (Field field : fileSchema.getFields()) {
        String value = null;
        if (codelists) {
          Optional<Restriction> optional = field.getRestriction(RestrictionType.CODELIST);
          if (optional.isPresent()) {
            Restriction restriction = optional.get();
            BasicDBObject config = restriction.getConfig();
            String codeListName = (String) config.get(CODELIST_NAME_RESTRICTION_PARAMETER);
            value = codeListName;
          }
        } else {
          ValueType valueType = field.getValueType();
          if (valueType != ValueType.TEXT) {
            value = valueType.toString();
          }
        }

        if (value != null) {
          list.add(new SimpleEntry<String, String>(field.getName(), value));
        }
      }
      sort(list, new Comparator<Entry<String, String>>() {

        @Override
        public int compare(Entry<String, String> entry1, Entry<String, String> entry2) {
          return entry1.getKey().compareTo(entry2.getKey());
        }
      });
      map.put(fileSchemaName, list);
    }

    for (val entry : map.entrySet()) {
      System.out.println(entry.getKey());
      List<Entry<String, String>> list = entry.getValue();
      for (val entry2 : list) {
        System.out.println(FIELD_SEPARATOR + on(FIELD_SEPARATOR).join(entry2.getKey(), entry2.getValue()));
      }
    }
  }

  private static boolean isStrictlyRequired(Dictionary dictionary, String fileSchemaName, String fieldName) {
    Field field = dictionary.getFileSchemaByName(fileSchemaName).get().field(fieldName).get();
    Optional<Restriction> optionalRequiredRestriction = field.getRestriction(RestrictionType.REQUIRED);

    boolean strictlyRequired = false;
    if (optionalRequiredRestriction.isPresent()) {
      Restriction requiredRestriction = optionalRequiredRestriction.get();
      strictlyRequired = isStrict(requiredRestriction);
    }
    return strictlyRequired;
  }

  private static void createDotVersion(Dictionary dictionary) {
    // TODO: CREATE file
    // TODO: formatting

    describe("Dictionary dot file");
    System.out.println("digraph dictionary {");
    System.out.println(" rankdir = BT");
    createVertices(dictionary);
    System.out.println();
    createEdges(dictionary);
    System.out.println("}");
  }

  private static void createVertices(Dictionary dictionary) {
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      String fileSchemaName = fileSchema.getName();
      String nodeColor = CLINICAL_FILE_SCHEMA_NAMES.contains(fileSchemaName) ? "Khaki1" : "AliceBlue";

      StringBuilder sb = new StringBuilder();
      sb.append(" " + fileSchemaName);
      sb.append(" [fontsize=8, ");
      sb.append("shape=note, ");
      sb.append("style=filled, fillcolor=\"" + nodeColor + "\", ");
      sb.append("label=\"");
      sb.append(fileSchemaName);

      List<String> uniqueFields = fileSchema.getUniqueFields();
      if (uniqueFields != null && uniqueFields.isEmpty() == false) {
        sb.append("\\n" + formatFields(uniqueFields));
      }
      sb.append("\"]");
      System.out.println(sb.toString());
    }
  }

  private static void createEdges(Dictionary dictionary) {
    for (FileSchema fileSchema : getFileSchemata(dictionary)) {
      String fileSchemaName = fileSchema.getName();

      for (Relation relation : fileSchema.getRelations()) {
        String other = relation.getOther();
        List<String> fields = relation.getFields();
        List<String> otherFields = relation.getOtherFields();

        boolean bidirectional = relation.isBidirectional();
        String edgeColor = bidirectional ? "Red" : "LightPink";
        String arrowType = bidirectional ? "normal" : "vee";

        if (RELATIONS_IGNORE_LIST.contains(other) == false) {
          String fieldsMapping =
              fields.equals(otherFields) ?
                  formatFields(fields) :
                  formatFields(fields) + "\\n->\\n" + formatFields(otherFields);
          StringBuilder sb = new StringBuilder();
          sb.append(" " + fileSchemaName + " -> " + other);
          sb.append(" [fontsize=8, ");
          sb.append("color=\"" + edgeColor + "\", ");
          sb.append("arrowhead = \"" + arrowType + "\", ");
          sb.append("label=\"");
          sb.append(fieldsMapping);
          if (bidirectional) {
            sb.append("\\nSURJECTIVE");
          }
          sb.append("\"]");
          System.out.println(sb.toString());
        }
      }
    }
  }

  private static boolean isStrict(Restriction requiredRestriction) {
    return !requiredRestriction.getConfig().getBoolean(ACCEPT_MISSING_CODE_RESTRICTION_PARAMETER);
  }

  private static String formatFields(List<String> fields) {
    return fields.toString().replace(", ", ",\\n");
  }

  private static List<FileSchema> getFileSchemata(Dictionary dictionary) {
    List<FileSchema> fileSchemata = newArrayList();
    for (FileSchema fileSchema : dictionary.getFiles()) {
      if (OPTIONAL_FILE_SCHEMA_NAMES.contains(fileSchema.getName()) == false) {
        fileSchemata.add(fileSchema);
      }
    }
    return fileSchemata;
  }

  private static void describe(String desc) {
    System.out.println(repeat("=", 75));
    System.out.println(desc);
    System.out.println();
  }

  @Data
  private static class FieldsRequiredness implements Comparable<FieldsRequiredness> {

    @NotNull
    private final String schemaName;

    @NotNull
    private final List<String> fields;

    @Override
    public String toString() {
      return on(FIELD_SEPARATOR).join(schemaName, fields);
    }

    @Override
    public int compareTo(FieldsRequiredness that) {
      return ComparisonChain.start()
          .compare(this.schemaName, that.schemaName)
          .compare(this.fields.toString(), that.fields.toString()) // Safe here; TODO: Ordering.natural()?
          .result();
    }
  }

  @Data
  private static class ReferencedFieldsUniqueness implements Comparable<ReferencedFieldsUniqueness> {

    @NotNull
    private final String other;

    @NotNull
    private final List<String> otherFields;

    @NotNull
    private final Boolean isUnique;

    @Override
    public String toString() {
      return on(FIELD_SEPARATOR).join(other, otherFields, isUnique);
    }

    @Override
    public int compareTo(ReferencedFieldsUniqueness that) {
      return ComparisonChain.start() //
          .compare(this.other, that.other) //
          .compare(this.otherFields.toString(), that.otherFields.toString()) // Safe here; TODO: Ordering.natural()?
          .result();
    }
  }

  @Data
  private static class RelationsSurjectivity implements Comparable<RelationsSurjectivity> {

    @NotNull
    private final String fileSchemaName;

    @NotNull
    private final String other;

    @NotNull
    private final Boolean bidirectional;

    @Override
    public String toString() {
      return on(FIELD_SEPARATOR).join(bidirectional, fileSchemaName + " -> " + other);
    }

    @Override
    public int compareTo(RelationsSurjectivity that) {
      return ComparisonChain.start() //
          .compare(this.fileSchemaName, that.fileSchemaName) //
          .compare(this.other, that.other) //
          .result();
    }
  }

  @Data
  private static class SchemaUniqueFieldsRequiredness implements Comparable<SchemaUniqueFieldsRequiredness> {

    @NotNull
    private final String fileSchemaName;

    @NotNull
    private final List<UniqueFieldsRequiredness> uniqueFieldsRequirednesss;

    @Override
    public String toString() {
      return fileSchemaName + "\n" + FIELD_SEPARATOR + uniqueFieldsRequirednesss;
    }

    @Override
    public int compareTo(SchemaUniqueFieldsRequiredness that) {
      return ComparisonChain.start() //
          .compare(this.fileSchemaName, that.fileSchemaName) //
          .result();
    }
  }

  @Data
  private static class UniqueFieldsRequiredness implements Comparable<UniqueFieldsRequiredness> {

    @NotNull
    private final String fieldName;

    @NotNull
    private final Boolean strictlyRequired;

    @Override
    public String toString() {
      return on(FIELD_SEPARATOR).join(fieldName, strictlyRequired);
    }

    @Override
    public int compareTo(UniqueFieldsRequiredness that) {
      return ComparisonChain.start() //
          .compare(this.fieldName, that.fieldName) //
          .result();
    }
  }
}
