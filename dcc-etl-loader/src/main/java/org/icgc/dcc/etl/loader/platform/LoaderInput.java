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
package org.icgc.dcc.etl.loader.platform;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.asMap;
import static com.google.common.collect.Maps.filterEntries;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.exists;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.isFile;
import static org.icgc.dcc.etl.core.util.EtlConventions.GENERATED_FILE_TYPE_TO_COMPONENT;
import static org.icgc.dcc.etl.core.util.EtlConventions.getConcatenatorProjectInputDir;
import static org.icgc.dcc.etl.core.util.EtlConventions.getInputGeneratingComponentProjectInputDir;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.FileTypes;
import org.icgc.dcc.common.core.model.FileTypes.FileType;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

/**
 * Gathers input for the loader (typically spread across several dirs).
 */
@NoArgsConstructor(access = PRIVATE)
public final class LoaderInput {

  public static Map<String, Map<FileType, String>> getInputFiles(
      @NonNull final FileSystem fileSystem,
      @NonNull final String inputDir,
      @NonNull final Set<String> projectKeys) {

    return ImmutableMap.<String, Map<FileType, String>> copyOf(asMap(
        projectKeys,
        new Function<String, Map<FileType, String>>() {

          @Override
          public Map<FileType, String> apply(String projectKey) {

            return getProjectInputFiles(
                fileSystem,
                inputDir,
                projectKey);
          }

        }));
  }

  private static Map<FileType, String> getProjectInputFiles(
      @NonNull final FileSystem fileSystem,
      @NonNull final String parentInputDir,
      @NonNull final String projectKey) {

    val concatenatedInputFiles = getConcatenatedInputFiles(
        fileSystem, parentInputDir, projectKey);

    val rewrittenInputFiles = getRewrittenInputFiles(
        fileSystem, parentInputDir, projectKey,
        concatenatedInputFiles);

    val builder = new ImmutableMap.Builder<FileType, String>();
    builder.putAll(filterEntries(
        concatenatedInputFiles,
        new Predicate<Entry<FileType, String>>() {

          @Override
          public boolean apply(Entry<FileType, String> entry) {
            return !hasRewrittenCounterpart(entry.getKey());
          }

          private boolean hasRewrittenCounterpart(FileType fileType) {
            return rewrittenInputFiles.keySet().contains(fileType);
          }

        }));
    builder.putAll(rewrittenInputFiles);

    return builder.build();
  }

  private static Map<FileType, String> getConcatenatedInputFiles(
      @NonNull final FileSystem fileSystem,
      @NonNull final String dataParentDir,
      @NonNull final String projectKey) {

    val builder = new ImmutableMap.Builder<FileTypes.FileType, String>();

    Path concatenatorProjectDir = new Path(getConcatenatorProjectInputDir(dataParentDir, projectKey));
    for (val fileType : FileType.values()) {
      Path concatenatedFile = new Path(concatenatorProjectDir, fileType.getHarmonizedOutputFileName());
      if (isFile(fileSystem, concatenatedFile)) {
        builder.put(fileType, concatenatedFile.toUri().toString());
      }
    }

    return builder.build();
  }

  private static Map<FileType, String> getRewrittenInputFiles(
      @NonNull final FileSystem fileSystem,
      @NonNull final String workingDir,
      @NonNull final String projectKey,
      @NonNull final Map<FileType, String> concatenatedInputFiles) {

    val builder = new ImmutableMap.Builder<FileTypes.FileType, String>();
    for (val entry : GENERATED_FILE_TYPE_TO_COMPONENT) {
      val fileType = entry.getKey();
      val component = entry.getValue();

      val alternative = new Path(
          new Path(getInputGeneratingComponentProjectInputDir(component, workingDir, projectKey)),
          fileType.getHarmonizedOutputFileName());

      if (exists(fileSystem, alternative)) { // Either file or directory (as MR part files)
        checkState(
            !isSubmissionFile(fileType)
                || concatenatedInputFiles.containsKey(fileType),
            "There should be a corresponding file for '%s' in '%s'",
            fileType, concatenatedInputFiles); // By design
        builder.put(fileType, alternative.toUri().toString());
      } else {
        checkState(!concatenatedInputFiles.containsKey(fileType),
            "There should not be a corresponding file for '%s' in '%s'",
            fileType, concatenatedInputFiles); // By design
      }
    }

    return builder.build();
  }

  private static boolean isSubmissionFile(FileType fileType) {
    return !fileType.isSimpleSecondary();
  }

}
