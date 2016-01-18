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
package org.icgc.dcc.etl.concatenator;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.getResource;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Files.contentOf;
import static org.icgc.dcc.common.core.util.Joiners.INDENT;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.submission.dictionary.util.Dictionaries;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
public class ConcatenatorTest {

  private static final String PARENT_DIR = getResource("fixtures/concatenator").getFile();
  private static final String PROJECTS_FILE = String.format("%s/%s", PARENT_DIR, "concatenator_projects.json");
  private static final List<String> PROJECTS = Arrays.asList("project1", "project2", "project3", "project4");

  /**
   * We can't use an actual {@link TemporaryFolder} because the above projects.json file must be able to point to
   * locations in it (it needs to be predictable).
   */
  private static final String TEMPORARY_DIR = "/tmp/dcc/concatenator";
  private static final String TEMPORARY_INPUT_DIR = String.format("%s/%s", TEMPORARY_DIR, "input");
  private static final String TEMPORARY_OUTPUT_DIR = String.format("%s/%s", TEMPORARY_DIR, "output");

  @Before
  public void setUp() throws IOException {
    FileUtils.deleteQuietly(new File(TEMPORARY_DIR));
    new File(TEMPORARY_DIR).mkdir();
    new File(TEMPORARY_INPUT_DIR).mkdir();
    new File(TEMPORARY_OUTPUT_DIR).mkdir();

    for (val project : PROJECTS) {
      val projectInputDir = String.format("%s/input/%s", PARENT_DIR, project);
      FileUtils.copyDirectory(
          new File(projectInputDir),
          new File(String.format("%s/%s", TEMPORARY_INPUT_DIR, project)));
    }

    log.info("Content: \n\t{}", INDENT.join(listFiles(new File(TEMPORARY_DIR), null, true)));
  }

  @Test
  public void testConcatenation() throws Exception {
    new Concatenator(
        FileSystem.get(new Configuration()),
        Dictionaries.readDccResourcesDictionary().getPatterns(),
        TEMPORARY_INPUT_DIR,
        PROJECTS_FILE,
        TEMPORARY_OUTPUT_DIR)
        .process();

    for (val project : PROJECTS) {
      val projectRefDir = String.format("%s/ref/%s", PARENT_DIR, project);
      val temporaryProjectOutputDir = String.format("%s/%s", TEMPORARY_OUTPUT_DIR, project);
      checkOutput(projectRefDir, temporaryProjectOutputDir);
    }

  }

  private void checkOutput(String projectRefDir, String temporaryProjectOutputDir) {
    log.info("Checking that actual output '{}' matches reference '{}'", temporaryProjectOutputDir, projectRefDir);
    for (val refFileName : new File(projectRefDir).list()) {
      val refFile = new File(projectRefDir, refFileName);
      val actualFile = new File(temporaryProjectOutputDir, refFileName);
      log.info("Checking '{}' versus '{}'", actualFile, refFile);
      assertThat(actualFile).exists();
      assertThat(contentOf(actualFile, UTF_8)).isEqualTo(contentOf(refFile, UTF_8));
    }
  }

  @After
  public void cleanUp() throws IOException {
    deleteTemporaryDir();
  }

  private void deleteTemporaryDir() throws IOException {
    FileUtils.deleteDirectory(new File(TEMPORARY_DIR));
  }

}
