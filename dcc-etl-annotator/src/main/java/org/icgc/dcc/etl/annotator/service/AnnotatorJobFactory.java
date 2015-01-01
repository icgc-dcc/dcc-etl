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
package org.icgc.dcc.etl.annotator.service;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Sets.intersection;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.isDirectory;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.lsAll;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.lsDir;
import static org.icgc.dcc.etl.core.model.Component.NORMALIZER;

import java.util.Collection;
import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.etl.annotator.model.AnnotatedFileType;
import org.icgc.dcc.etl.annotator.model.AnnotatorJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @_(@Autowired))
public class AnnotatorJobFactory {

  @NonNull
  private FileSystem fileSystem;

  public AnnotatorJob createAnnotatorJob(String workingDir, Iterable<String> projectNames,
      Iterable<AnnotatedFileType> fileTypes) {
    if (isLocal(fileSystem)) {
      log.debug("Resolving annotation files on [LOCAL] filesystem.");
    } else {
      log.debug("Resolving annotation files on [DISTRIBUTED] filesystem.");
    }

    val effectiveProjectNames = resolveEffectiveProjectNames(workingDir, projectNames);
    log.info("Resolved the following effective projects given the specified and available projects: {}",
        effectiveProjectNames);

    return AnnotatorJob.builder()
        .projectNames(effectiveProjectNames)
        .fileTypes(fileTypes)
        .files(resolveFiles(workingDir, effectiveProjectNames, fileTypes))
        .workingDir(workingDir)
        .build();
  }

  private Iterable<String> resolveEffectiveProjectNames(String workingDir, Iterable<String> projectNames) {
    val availableProjectNames = resolveAvailableProjectNames(workingDir);
    log.debug("Resolved available project names: {}", projectNames);

    if (isEmpty(projectNames)) {
      log.debug("Received an empty projects list. Using all available projects");
      return availableProjectNames;
    } else {
      log.debug("Received a non-empty projects list. Using the intersection of specified and available projects");
      return intersection(ImmutableSet.copyOf(availableProjectNames), ImmutableSet.copyOf(projectNames));
    }
  }

  private Iterable<String> resolveAvailableProjectNames(String workingDir) {
    val projectsDir = new Path(workingDir, NORMALIZER.getDirName());
    val projectNames = lsDir(fileSystem, projectsDir);
    log.debug("Projects directory '{}' contents: '{}'", projectsDir, projectNames);
    val result = new ImmutableList.Builder<String>();

    for (val projectName : projectNames) {
      if (isDirectory(fileSystem, projectName)) {
        result.add(projectName.getName());
      }
    }

    return result.build();
  }

  private Collection<Path> resolveFiles(String rootDir, Iterable<String> projectNames,
      Iterable<AnnotatedFileType> fileTypes) {
    log.info("Creating files. Initial params: {}, {}, {}", new Object[] { rootDir, projectNames, fileTypes });
    val workingDir = new Path(rootDir, NORMALIZER.getDirName());

    val result = new ImmutableList.Builder<Path>();
    for (val projectName : projectNames) {
      val projectDir = new Path(workingDir, projectName);
      val dirContents = lsAll(fileSystem, projectDir);
      log.debug("Contents of [{}] project directory: '{}'", projectName, dirContents);
      result.addAll(resolveProjectFiles(projectDir, fileTypes));
    }

    return result.build();
  }

  @SneakyThrows
  private List<Path> resolveProjectFiles(Path projectDir, Iterable<AnnotatedFileType> fileTypes) {
    val result = new ImmutableList.Builder<Path>();
    for (val fileType : fileTypes) {
      val filePath = new Path(projectDir, fileType.getInputFileName());
      if (fileSystem.exists(filePath)) {
        log.debug("Adding '{}' for annotation.", filePath);
        result.add(filePath);
      } else {
        log.debug("'{}' does not exist. Skipping.", filePath);
      }
    }

    return result.build();
  }

  private static boolean isLocal(FileSystem fileSystem) {
    return fileSystem instanceof LocalFileSystem;
  }

}
