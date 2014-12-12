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
package org.icgc.dcc.etl.pruner;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.io.IOException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.common.hadoop.dcc.SubmissionInputData;
import org.icgc.dcc.etl.pruner.core.OrphanAnalyzer;
import org.icgc.dcc.etl.pruner.core.OrphanRemover;
import org.icgc.dcc.submission.dictionary.model.Dictionary;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

/**
 * Generates reports for donors with no data and prunes them before further processing in the ETL. (DCC-2546).
 */
@Slf4j
@RequiredArgsConstructor
public class Pruner {

  /**
   * Dependencies.
   */
  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final Dictionary dictionary;

  public void prune(@NonNull String inputDirectory, @NonNull String projectsJsonFilePath) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();

    val projectNames = SubmissionInputData.getMatchingFiles(
        fileSystem,
        inputDirectory,
        projectsJsonFilePath,
        dictionary.getPatterns()).keySet();

    val orphans = new OrphanAnalyzer(fileSystem, dictionary).analyze(inputDirectory, projectNames);
    new OrphanRemover(fileSystem, dictionary).remove(orphans);

    log.info("Pruning {} orphans took: {}", formatCount(Iterables.size(orphans)), timer.stop());
  }

}
