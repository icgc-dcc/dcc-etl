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
package org.icgc.dcc.etl.pruner.core;

import static org.icgc.dcc.etl.pruner.util.OrphanIndexes.groupOrphansByFile;
import static org.icgc.dcc.etl.pruner.util.OrphanIndexes.groupOrphansByFileType;
import static org.icgc.dcc.etl.pruner.util.OrphanIndexes.groupOrphansByProject;
import static org.icgc.dcc.etl.pruner.util.Orphans.getOrphanIds;

import java.io.IOException;
import java.util.Collections;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.etl.pruner.model.Orphan;
import org.icgc.dcc.submission.dictionary.model.Dictionary;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Reports the number of {@link Orphan}s for projects.
 */
@RequiredArgsConstructor
public class OrphanReporter {

  /**
   * Dependencies.
   */
  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final Dictionary dictionary;

  public void report(@NonNull Iterable<Orphan> orphans) throws IOException {
    val orphansByProject = groupOrphansByProject(orphans);
    val projectNames = Lists.<String> newArrayList(orphansByProject.keySet());
    Collections.sort(projectNames);
    output("Project\tDonor\tSpecimen\tSample");
    for (val projectName : projectNames) {
      output("\n" + projectName);
      val orphansByFileType = groupOrphansByFileType(orphansByProject.get(projectName));
      for (val fileType : orphansByFileType.keySet()) {
        val orphansByFile = groupOrphansByFile(orphansByFileType.get(fileType));
        for (val inputFilePath : orphansByFile.keySet()) {
          val orphansToReport = orphansByFile.get(inputFilePath);
          val orphanedIds = getOrphanIds(orphansToReport);
          output("\t" + Iterables.size(orphanedIds));
        }
      }
    }
    output("\n");
  }

  private void output(String output) {
    System.out.print(output);
  }
}
