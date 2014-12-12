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
package org.icgc.dcc.etl.annotator;

import static org.apache.commons.io.FileUtils.copyDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.annotator.cli.Options;
import org.icgc.dcc.etl.annotator.model.AnnotatedFileType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.beust.jcommander.JCommander;

@Slf4j
public class AnnotatorMainTest {

  private static final File FIXTURE_DIR = new File("src/test/resources/fixtures");

  @Rule
  TemporaryFolder tmp = new TemporaryFolder();
  File workingDir;

  @Before
  public void setUp() throws IOException {
    val testDir = new File(FIXTURE_DIR, "icgc/overarch/ICCG17/0/0");
    val workingDir = tmp.newFolder();

    log.info("Copying files from '{}' to '{}'...", testDir, workingDir);
    copyDirectory(testDir, workingDir);

    this.workingDir = workingDir;
  }

  @Test
  public void testMain() throws IOException {
    AnnotatorMain.main(
        "--working-dir", workingDir.getAbsolutePath(),
        "--project-names", "ALL-US");
  }

  @Test
  public void argumentsTest() {
    Options options = parseOptions("--working-dir", "value");
    assertThat(options.help).isFalse();
    assertThat(options.version).isFalse();
    assertThat(options.workingDir).isEqualTo("value");
    assertThat(options.projectNames).isEmpty();
    assertThat(options.fileTypes).containsOnly(AnnotatedFileType.values());

    options = parseOptions("--working-dir", "value", "--project-names", "P1,P2");
    assertThat(options.help).isFalse();
    assertThat(options.version).isFalse();
    assertThat(options.workingDir).isEqualTo("value");
    assertThat(options.projectNames).containsOnly("P1", "P2");
    assertThat(options.fileTypes).containsOnly(AnnotatedFileType.values());
  }

  @Test
  public void fileTypesArgumentsTest() {
    val options = parseOptions("--working-dir", "value", "--file-types", "ssm,sgv");
    assertThat(options.help).isFalse();
    assertThat(options.version).isFalse();
    assertThat(options.workingDir).isEqualTo("value");
    assertThat(options.projectNames).isEmpty();
    assertThat(options.fileTypes).containsOnly(AnnotatedFileType.values());
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileTypesExtraArgumentTest() {
    val options = parseOptions("--working-dir", "value", "--file-types", "ssm,sgv,something");
    assertThat(options.help).isFalse();
    assertThat(options.version).isFalse();
    assertThat(options.workingDir).isEqualTo("value");
    assertThat(options.projectNames).isEmpty();
    assertThat(options.fileTypes).containsOnly(AnnotatedFileType.values());
  }

  private static Options parseOptions(String... args) {
    val options = new Options();
    new JCommander(options, args);

    return options;
  }

}
