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

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.mkdirs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.hadoop.dcc.SubmissionInputData;

/**
 * Concatenates and uncompress submission files for further processing in the ETL.
 */
@Slf4j
@RequiredArgsConstructor
public class Concatenator {

  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final Map<FileType, String> patterns;

  /**
   * Default location for projects that dont specify a {@link #PARENT_DIR_PARAMETER}, typically something like
   * "/icgc/submission/ICGC16". It will be augmented with the project key, for instance
   * "/icgc/submission/ICGC16/BRCA-UK".
   */
  @NonNull
  private final String defaultParentDataDir;

  /**
   * Keys for the path to the data. "*_file" takes precedence over "data_dir" (whether "data_dir" is specified or not).
   * 
   * <pre>
   * {
   *   "COAD-US": {
   *     "cnsm_m_file": "/nfs/dcc_secure/dcc/staging_14_15/cnsm_m.txt" // may overwrite any
   *     ...
   *   }, ...
   * }
   * </pre>
   */
  @NonNull
  private final String projectsJsonFilePath;
  @NonNull
  private final String outputDirPath;

  public void process() {
    processFiles(
        fileSystem,
        outputDirPath,
        SubmissionInputData.getMatchingFiles(
            fileSystem,
            defaultParentDataDir,
            projectsJsonFilePath,
            patterns));
  }

  /**
   * Actually carries out the uncompressing/concatenation based on the pre-computed matching files map.
   */
  private static void processFiles(
      FileSystem fileSystem,
      String outputDirPath,
      Map<String, Map<FileType, List<Path>>> matchingFiles) {
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    log.info("Creating pool of '{}' threads", availableProcessors);
    val pool = newFixedThreadPool(availableProcessors);

    for (val projectKey : matchingFiles.keySet()) {
      val projectOutputDir = new Path(outputDirPath, projectKey);
      val fileTypeToFiles = matchingFiles.get(projectKey);

      log.info("Ensuring '{}' exists", projectOutputDir);
      mkdirs(fileSystem, projectOutputDir);

      for (val fileType : fileTypeToFiles.keySet()) {
        val inputFiles = fileTypeToFiles.get(fileType);
        if (!inputFiles.isEmpty()) {
          val outputFilePath = new Path(projectOutputDir, fileType.getHarmonizedOutputFileName());
          pool.execute(new ConcatenatorJob(fileSystem, inputFiles, outputFilePath));
        }
      }
    }

    pool.shutdown();
    awaitTermination(pool);
  }

  @SneakyThrows
  private static void awaitTermination(ExecutorService pool) {
    log.info("Awaiting termination");
    pool.awaitTermination(MAX_VALUE, NANOSECONDS);
  }

}
