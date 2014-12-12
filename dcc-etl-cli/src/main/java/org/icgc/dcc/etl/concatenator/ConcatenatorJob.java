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
package org.icgc.dcc.etl.concatenator;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.hadoop.parser.FileRecordProcessor;
import org.icgc.dcc.common.hadoop.parser.TsvPartFileProcessor;

/**
 * Concatenates/uncompress a list of input files and writes corresponding output file.
 */
@Slf4j
@RequiredArgsConstructor
public class ConcatenatorJob implements Runnable {

  private final FileSystem fileSystem;
  private final List<Path> inputFiles;
  private final Path outputFilePath;

  @Override
  public void run() {
    log.info("Proceeding with concatenation/uncompressing for '{}'", inputFiles);
    concatenateFiles();
  }

  private void concatenateFiles() {

    @Cleanup
    val concatWriter = getWriter();

    TsvPartFileProcessor.parseFiles(
        fileSystem,
        inputFiles,
        new FileRecordProcessor<String>() {

          boolean firstPart = true;

          @Override
          public void process(long lineNumber, String record) throws IOException {
            if (lineNumber == 1) {
              if (firstPart) {
                firstPart = false;
              } else {
                return;
              }
            }

            concatWriter.println(record);
          }

        });
  }

  @SneakyThrows
  private PrintWriter getWriter() {
    return new PrintWriter(fileSystem.create(outputFilePath));
  }

}
