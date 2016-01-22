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
package org.icgc.dcc.etl.pruner.core;

import static com.google.common.io.Resources.getResource;
import static org.apache.commons.io.FileUtils.copyDirectory;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.assertj.core.api.Assertions.assertThat;

import static org.icgc.dcc.common.core.util.Joiners.INDENT;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.submission.dictionary.util.Dictionaries;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

@Slf4j
public class OrphanRemoverTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static final String PARENT_DIR = getResource("fixtures/pruner").getFile();
  private static final Map<String, Integer> PROJECTS_ORPHANED_DONORS_COUNT = ImmutableMap.of("ALL-US", 213, "LAML-KR",
      19, "EOPC-DE", 6, "PRAD-CA", 2);

  @Test
  public void testRemoverAnalyzer() throws Exception {
    val input = temporaryFolder.newFolder("input");
    val fileSystem = FileSystem.get(new Configuration());
    val dictionary = Dictionaries.readDccResourcesDictionary();
    val projectNames = PROJECTS_ORPHANED_DONORS_COUNT.keySet();

    copyTestDataToTemp(input);
    log.info("Content: \n\t{}", INDENT.join(listFiles(temporaryFolder.getRoot(), null, true)));

    val orphans = new OrphanAnalyzer(fileSystem, dictionary).analyze(input.getAbsolutePath(), projectNames);
    new OrphanRemover(fileSystem, dictionary).remove(orphans);

    val afterRemovalOrphans =
        new OrphanAnalyzer(fileSystem, dictionary).analyze(input.getAbsolutePath(), projectNames);
    assertThat(afterRemovalOrphans).isEmpty();

  }

  private static void copyTestDataToTemp(File tempFolder) throws IOException {
    for (val projectName : PROJECTS_ORPHANED_DONORS_COUNT.keySet()) {
      String projectInputDir = String.format("%s/input/%s", PARENT_DIR, projectName);
      String targetDirectory = String.format("%s/%s", tempFolder.getAbsolutePath(), projectName);
      copyDirectory(
          new File(projectInputDir),
          new File(targetDirectory));
    }
  }

}
