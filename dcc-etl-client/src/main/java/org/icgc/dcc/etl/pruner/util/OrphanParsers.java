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
package org.icgc.dcc.etl.pruner.util;

import static lombok.AccessLevel.PRIVATE;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.hadoop.parser.FileParser;
import org.icgc.dcc.common.hadoop.parser.FileRecordProcessor;
import org.icgc.dcc.etl.pruner.model.Line;
import org.icgc.dcc.submission.core.parser.SubmissionFileParsers;
import org.icgc.dcc.submission.dictionary.model.FileSchema;

import com.google.common.collect.ImmutableMap;

/**
 * Common utilities for parsing feature files.
 */
@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class OrphanParsers {

  @SneakyThrows
  public static Map<String, Line> parseFileFields(@NonNull FileSystem fileSystem,
      @NonNull final FileType fileType, @NonNull final String keyFieldName, @NonNull final String valueFieldName,
      @NonNull FileSchema fileSchema, @NonNull final Path file) {
    val lines = ImmutableMap.<String, Line> builder();
    val fileParser = newMapFileParser(fileSystem, fileSchema);
    fileParser.parse(file, new FileRecordProcessor<Map<String, String>>() {

      @Override
      public void process(long lineNumber, Map<String, String> record) throws IOException {
        val line =
            new Line(fileType, file, lineNumber, record.get(valueFieldName));
        lines.put(record.get(keyFieldName), line);
      }

    });

    return lines.build();
  }

  @SneakyThrows
  public static Map<String, Line> parseFileField(@NonNull FileSystem fileSystem,
      @NonNull final FileType fileType, @NonNull final String fieldName, @NonNull FileSchema fileSchema,
      @NonNull final Path file) {
    log.info("Reading file {}", file.toString());
    val lines = new HashMap<String, Line>();
    val fileParser = newMapFileParser(fileSystem, fileSchema);
    fileParser.parse(file, new FileRecordProcessor<Map<String, String>>() {

      @Override
      public void process(long lineNumber, Map<String, String> record) throws IOException {
        val line = new Line(fileType, file, lineNumber);
        lines.put(record.get(fieldName), line);
      }

    });

    return ImmutableMap.copyOf(lines);
  }

  public static FileParser<Map<String, String>> newMapFileParser(@NonNull FileSystem fileSystem,
      @NonNull FileSchema fileSchema) {
    val fileParser = SubmissionFileParsers.newMapFileParser(fileSystem, fileSchema);
    return fileParser;
  }

}
