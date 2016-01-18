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
package org.icgc.dcc.etl.annotator.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl.annotator.model.AnnotatedFileType.SGV;
import static org.icgc.dcc.etl.annotator.model.AnnotatedFileType.SSM;
import static org.icgc.dcc.etl.core.model.Component.NORMALIZER;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;

import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.etl.annotator.model.AnnotatedFileType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;

public class AnnotatorJobFactoryTest {

  private static final List<String> PROJECTS = ImmutableList.of();
  private static final List<AnnotatedFileType> ALL_FILE_TYPES = Lists.newArrayList(AnnotatedFileType.values());

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();
  FileSystem fileSystem;
  AnnotatorJobFactory jobFactory;

  @Before
  @SneakyThrows
  public void setUp() {
    tmp.create();
    this.fileSystem = FileSystem.getLocal(new Configuration());
  }

  @After
  public void tearDown() {
    tmp.delete();
  }

  @SneakyThrows
  private void setUpFiles() {
    val workingDirName = NORMALIZER.getDirName();
    tmp.newFolder(workingDirName);
    val bothPath = workingDirName + File.separator + "BOTH";
    tmp.newFolder(bothPath);
    tmp.newFile(bothPath + File.separator + SSM.getInputFileName());
    tmp.newFile(bothPath + File.separator + SGV.getInputFileName());

    val ssmPath = workingDirName + File.separator + "SSM";
    tmp.newFolder(ssmPath);
    tmp.newFile(ssmPath + File.separator + SSM.getInputFileName());

    val sgvPath = workingDirName + File.separator + "SGV";
    tmp.newFolder(sgvPath);
    tmp.newFile(sgvPath + File.separator + SGV.getInputFileName());
  }

  @Test
  public void createAnnotatorJobTest() {
    setUpFiles();

    val workingDirPath = tmp.getRoot().getAbsolutePath();
    val normalizerPath = workingDirPath + File.separator + NORMALIZER.getDirName();
    val expectedResults = createResultList(normalizerPath);
    jobFactory = new AnnotatorJobFactory(fileSystem);

    val actualResult = jobFactory.createAnnotatorJob(workingDirPath, PROJECTS, ALL_FILE_TYPES);
    assertThat(actualResult.getFiles()).containsOnlyElementsOf(expectedResults);
  }

  @SneakyThrows
  private void setupFsMock(List<Path> paths) {
    for (val path : paths) {
      when(fileSystem.exists(path)).thenReturn(true);
    }
  }

  private static Path createPath(String workingDir, String project, String file) {
    val projectDir = new Path(workingDir, project);

    return new Path(projectDir, file);
  }

  private static List<Path> createResultList(String normalizerPath) {
    return ImmutableList.of(
        createPath(normalizerPath, "BOTH", AnnotatedFileType.SGV.getInputFileName()),
        createPath(normalizerPath, "BOTH", AnnotatedFileType.SSM.getInputFileName()),
        createPath(normalizerPath, "SSM", AnnotatedFileType.SSM.getInputFileName()),
        createPath(normalizerPath, "SGV", AnnotatedFileType.SGV.getInputFileName()));
  }

}
