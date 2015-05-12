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
package org.icgc.dcc.etl.stats;

import static com.google.common.collect.Iterables.transform;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.DONOR_TYPE;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Set;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.etl.loader.platform.LoaderInput;

import com.google.common.base.Function;

/**
 * Gathers statistics about HDFS entities.
 */
@Slf4j
public class HdfsStats {

  private final String dataParentDir;

  private final Set<String> projectKeys;

  private final FileSystem fileSystem;

  @SneakyThrows
  HdfsStats(@NonNull String uri, @NonNull String dataParentDir, @NonNull Set<String> projectKeys) {
    this.dataParentDir = dataParentDir;
    this.projectKeys = projectKeys;
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", uri);
    log.info("Using uri: {}", uri);
    this.fileSystem = FileSystem.get(configuration);
  }

  @SneakyThrows
  void stats(StatsResults results) { // TODO: CREATE abstract method (all 3 *Stats have a stat() method and a uri)

    for (val donorFilePath : getDonorFiles()) {
      val donorFile = new Path(donorFilePath);
      long lineCount = wcLines(fileSystem, donorFile);
      results.hdfsDonorCounts.put(
          donorFile.getParent().toUri().toString(),
          lineCount - 1); // -1 for header
    }
  }

  private Iterable<String> getDonorFiles() {
    val inputFiles = LoaderInput.getInputFiles(fileSystem, dataParentDir, projectKeys);

    return transform(
        projectKeys,
        new Function<String, String>() {

          @Override
          public String apply(String projectKey) {
            return inputFiles.get(projectKey).get(DONOR_TYPE);
          }

        });
  }

  private static long wcLines(FileSystem fileSystem, Path filePath) throws IOException {
    @Cleanup
    LineNumberReader lnr = new LineNumberReader(new InputStreamReader(fileSystem.open(filePath)));
    lnr.skip(Long.MAX_VALUE);
    return lnr.getLineNumber();
  }

}
