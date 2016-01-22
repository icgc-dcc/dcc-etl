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
package org.icgc.dcc.etl.loader.flow;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.common.core.model.FileTypes.NOT_APPLICABLE;
import static org.icgc.dcc.common.cascading.Fields2.getFieldName;

import java.util.List;

import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.cascading.Fields2;

import cascading.tuple.Fields;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * A utility class for fields in the context of the loader.
 * <p>
 * TODO: See DCC-710
 */
public class LoaderFields {

  public static String prefixedFieldName(FileType fileType, String fieldName) {
    return Fields2.prefixedFieldName(fileType, fieldName);
  }

  public static Fields prefixedFields(FileType type, List<String> fieldNames) {
    return prefixedFields(type.getId(), fieldNames);
  }

  public static Fields prefixedFields(FileType type, String field) {
    return prefixedFields(type.getId(), field);
  }

  public static Fields prefixedFields(FileType type, Fields field) {
    Fields2.checkFieldsCardinalityOne(field);
    return prefixedFields(type.getId(), getFieldName(field));
  }

  /**
   * TODO: remove and replace with the one using {@link FileType} instead.
   */
  public static Fields prefixedFields(String prefix, String field) {
    return Fields2.prefixedFields(prefix, field);
  }

  public static Fields generatedFields(String... names) {
    Fields generatedFields = new Fields(); // TODO: better way to get an instance?
    for (String name : names) {
      generatedFields = generatedFields.append(generatedFields(name));
    }
    return generatedFields;
  }

  public static Fields generatedFields(String name) {
    return prefixedFields(NOT_APPLICABLE, name);
  }

  /**
   * See {@link #generatedFieldName(FileType)}.
   */
  public static Fields generatedField(FileType fileType) {
    return new Fields(generatedFieldName(fileType));
  }

  public static Fields generatedField(String fieldName) {
    return new Fields(generatedFieldName(fieldName));
  }

  /**
   * Typically used for nesting fields
   */
  public static String generatedFieldName(FileType fileType) {
    return generatedFieldName(fileType.getId());
  }

  public static String generatedFieldName(String fieldName) {
    return Fields2.prefixedFieldName(NOT_APPLICABLE, fieldName);
  }

  /**
   * Should avoid using this method directly from now on, favour {@link #prefixedFields(FileType, String)} and the
   * likes.
   */
  public static Fields prefixedFields(final String prefix, Iterable<? extends Comparable<?>> fields) {
    Iterable<String> transform =
        Iterables.transform(fields, new com.google.common.base.Function<Comparable<?>, String>() {

          @Override
          public String apply(Comparable<?> input) {
            checkNotNull(input);
            return Fields2.prefix(prefix, input.toString());
          }
        });
    return new Fields(Iterables.toArray(transform, String.class));
  }

  public static List<String> prefixFieldNames(
      final FileType fileType,
      final Iterable<String> fieldNames) {

    return newArrayList(transform(
        fieldNames,
        new Function<String, String>() {

          @Override
          public String apply(String fieldName) {
            return prefixedFieldName(fileType, fieldName);
          }

        }));
  }

  public static String unprefixFieldName(String fieldName) {
    return getFieldName(unprefixFields(new Fields(fieldName)));
  }

  public static Fields unprefixFields(Fields fields) {
    return Fields2.unprefixFields(fields);
  }

  public static String getPrefix(String prefixedFieldName) {
    return Fields2.getPrefix(prefixedFieldName);
  }

  public static Function<String, String> getGeneratedFieldName() {

    return new Function<String, String>() {

      @Override
      public String apply(String fieldName) {
        return generatedFieldName(fieldName);
      }

    };
  }

}
