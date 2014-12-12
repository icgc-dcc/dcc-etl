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

import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.collect.Iterables.contains;
import static org.icgc.dcc.common.core.util.Joiners.TAB;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.mkdirs;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.mv;
import static org.icgc.dcc.etl.pruner.util.OrphanIndexes.groupOrphansByFile;
import static org.icgc.dcc.etl.pruner.util.OrphanIndexes.groupOrphansByFileType;
import static org.icgc.dcc.etl.pruner.util.OrphanIndexes.groupOrphansByProject;
import static org.icgc.dcc.etl.pruner.util.OrphanParsers.newMapFileParser;
import static org.icgc.dcc.etl.pruner.util.OrphanWriters.getWriter;
import static org.icgc.dcc.etl.pruner.util.Orphans.FILETYPE_ID_FIELD_NAMES;
import static org.icgc.dcc.etl.pruner.util.Orphans.PRUNER_DIR;
import static org.icgc.dcc.etl.pruner.util.Orphans.getOrphanIds;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.hadoop.parser.FileRecordProcessor;
import org.icgc.dcc.etl.pruner.model.Orphan;
import org.icgc.dcc.submission.dictionary.model.Dictionary;

import com.google.common.collect.Multimap;

/**
 * Removes {@link Orphan}s from clinical files.
 */
@Slf4j
@RequiredArgsConstructor
public class OrphanRemover {

  /**
   * Dependencies.
   */
  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final Dictionary dictionary;

  public void remove(@NonNull Iterable<Orphan> orphans) throws IOException {
    val timer = createStarted();
    val orphansByProject = groupOrphansByProject(orphans);
    removeProjectOrphans(orphansByProject);
    log.info("Finished removing orphans in {}", timer.stop());
  }

  private void removeProjectOrphans(Multimap<String, Orphan> orphansByProject)
      throws IOException {
    for (val projectName : orphansByProject.keySet()) {
      log.info("Processing Project '{}' ", projectName);
      val orphansByFileType = groupOrphansByFileType(orphansByProject.get(projectName));
      removeFileTypeOrphans(projectName, orphansByFileType);
      log.info("Finished processing Project '{}' ", projectName);
    }
  }

  private void removeFileTypeOrphans(String projectName,
      Multimap<FileType, Orphan> orphansByFileType) throws IOException {
    for (val fileType : orphansByFileType.keySet()) {
      val orphansByFile = groupOrphansByFile(orphansByFileType.get(fileType));
      removeFileOrphans(projectName, fileType, orphansByFile);
    }
  }

  private void removeFileOrphans(String projectName, FileType fileType,
      Multimap<Path, Orphan> orphansByFile) throws IOException {
    for (val inputFilePath : orphansByFile.keySet()) {
      log.info("Removing orphans from '{}' ", inputFilePath.getName());
      val orphansToRemove = orphansByFile.get(inputFilePath);
      val orphanedIds = getOrphanIds(orphansToRemove);
      remove(fileSystem, dictionary, fileType, inputFilePath, orphanedIds);
    }
  }

  private static void remove(FileSystem fileSystem, Dictionary dictionary, final FileType fileType, Path inputFile,
      final Iterable<String> orphanedIds) throws IOException {
    val prunerBackupDir = new Path(inputFile.getParent(), PRUNER_DIR);
    mkdirs(fileSystem, prunerBackupDir);
    val movedInputFile = new Path(prunerBackupDir, inputFile.getName());
    mv(fileSystem, inputFile, movedInputFile);

    @Cleanup
    val writer = getWriter(fileSystem, inputFile);

    val fileParser = newMapFileParser(fileSystem, dictionary.getFileSchema(fileType));
    fileParser.parse(movedInputFile, new FileRecordProcessor<Map<String, String>>() {

      boolean first = true;

      @Override
      public void process(long lineNumber, Map<String, String> record) throws IOException {
        if (first) {
          first = false;
          writer.println(formatTsv(record.keySet()));
        }
        if (!isOrphaned(record, orphanedIds, fileType)) {
          writer.println(formatTsv(record.values()));
        }
      }

      private boolean isOrphaned(Map<String, String> record, Iterable<String> orphanedIds, FileType fileType) {
        val idHeader = FILETYPE_ID_FIELD_NAMES.get(fileType);
        val id = record.get(idHeader);
        return contains(orphanedIds, id);
      }

      private String formatTsv(Collection<String> collection) {
        return TAB.join(collection);
      }

    });

  }
}
