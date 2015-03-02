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
package org.icgc.dcc.etl.db.importer.util;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

@RequiredArgsConstructor
public abstract class AbstractTsvMapReader {

  /**
   * Constants.
   */
  private static final char FIELD_SEPARATOR = '\t';
  private static final TypeReference<Map<String, String>> RECORD_TYPE_REFERENCE =
      new TypeReference<Map<String, String>>() {};

  @SneakyThrows
  protected Iterable<Map<String, String>> readRecords(InputStream inputStream) {
    val reader = createReader(CsvSchema.emptySchema().withHeader());

    return readRecords(inputStream, reader);
  }

  @SneakyThrows
  protected Iterable<Map<String, String>> readRecords(String[] columnNames, InputStream inputStream) {
    val reader = createReader(columnNames);

    return readRecords(inputStream, reader);
  }

  private Iterable<Map<String, String>> readRecords(InputStream inputStream, ObjectReader reader) throws IOException,
      JsonProcessingException {
    MappingIterator<Map<String, String>> values = reader.readValues(inputStream);

    return once(values);
  }

  private ObjectReader createReader(String[] columnNames) {
    val schema = CsvSchema.builder();
    for (val columnName : columnNames) {
      schema.addColumn(columnName);
    }

    return createReader(schema.build());
  }

  private ObjectReader createReader(CsvSchema schema) {
    return new CsvMapper()
        .reader(RECORD_TYPE_REFERENCE)
        .with(
            schema
                .withColumnSeparator(FIELD_SEPARATOR));
  }

  public static <T> Iterable<T> once(final Iterator<T> source) {
    return new Iterable<T>() {

      private AtomicBoolean exhausted = new AtomicBoolean();

      @Override
      public Iterator<T> iterator() {
        checkState(!exhausted.getAndSet(true));

        return source;
      }

    };
  }

}
