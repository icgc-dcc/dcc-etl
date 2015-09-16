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

import static com.google.common.io.Resources.getResource;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.assertj.core.api.Assertions.assertThat;

import static org.icgc.dcc.common.core.util.Joiners.INDENT;
import static org.icgc.dcc.etl.pruner.util.OrphanIndexes.groupOrphansByProject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.etl.pruner.model.Orphan;
import org.icgc.dcc.submission.dictionary.util.Dictionaries;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

@Slf4j
public class OrphanAnalyzerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static final String PARENT_DIR = getResource("fixtures/pruner").getFile();
  private static final Map<String, Integer> PROJECTS_ORPHANED_DONORS_COUNT = ImmutableMap.of("ALL-US", 213, "LAML-KR",
      19, "EOPC-DE", 6, "PRAD-CA", 2);

  @Test
  public void testOrphanAnalyzer() throws Exception {
    val input = temporaryFolder.newFolder("input");
    val fileSystem = FileSystem.get(new Configuration());
    val dictionary = Dictionaries.readDccResourcesDictionary();
    val projectNames = PROJECTS_ORPHANED_DONORS_COUNT.keySet();

    copyTestDataToTemp(input);
    log.info("Content: \n\t{}", INDENT.join(listFiles(temporaryFolder.getRoot(), null, true)));

    val orphans =
        new OrphanAnalyzer(fileSystem, dictionary).analyze(input.getAbsolutePath(), projectNames);
    val orphansByProject = groupOrphansByProject(orphans);

    assertThat(orphansByProject.keySet()).hasSameElementsAs(projectNames);

    val testDataOrphans = getTestData(input);
    val testDataOrphansByProject = groupOrphansByProject(testDataOrphans);

    for (val projectName : projectNames) {
      val projectOrphanedDonorCount = PROJECTS_ORPHANED_DONORS_COUNT.get(projectName);
      val orphansForProject = orphansByProject.get(projectName);
      assertThat(orphansForProject.size() == projectOrphanedDonorCount);

      val testDataOrphansForProject = testDataOrphansByProject.get(projectName);
      assertThat(orphansForProject).containsAll(testDataOrphansForProject);

      // for PRAD-CA project, test data is exact.
      if (projectName.equals("PRAD-CA")) {
        assertThat(orphansForProject).hasSameElementsAs(testDataOrphansForProject);
      }
    }
  }

  private static void copyTestDataToTemp(File tempFolder) throws IOException {
    for (val projectName : PROJECTS_ORPHANED_DONORS_COUNT.keySet()) {
      String projectInputDir = String.format("%s/input/%s", PARENT_DIR, projectName);
      String targetDirectory = String.format("%s/%s", tempFolder.getAbsolutePath(), projectName);
      FileUtils.copyDirectory(
          new File(projectInputDir),
          new File(targetDirectory));
    }
  }

  private static List<Orphan> getTestData(File input) {
    List<Orphan> list = new ArrayList<Orphan>();
    Path inputPath = new Path("file://", input.getAbsolutePath());

    list.add(Orphan.builder()
        .projectName("PRAD-CA")
        .fileType(FileType.DONOR_TYPE)
        .file(new Path(inputPath, "PRAD-CA/donor.txt"))
        .id("CPCG0001")
        .lineNumber(2)
        .build());
    list.add(Orphan.builder()
        .projectName("PRAD-CA")
        .fileType(FileType.SPECIMEN_TYPE)
        .file(new Path(inputPath, "PRAD-CA/specimen.txt"))
        .id("CPCG0001-B1")
        .lineNumber(2)
        .build());
    list.add(Orphan.builder()
        .projectName("PRAD-CA")
        .fileType(FileType.SPECIMEN_TYPE)
        .file(new Path(inputPath, "PRAD-CA/specimen.txt"))
        .id("CPCG0001-F1")
        .lineNumber(3)
        .build());
    list.add(Orphan.builder()
        .projectName("PRAD-CA")
        .fileType(FileType.SAMPLE_TYPE)
        .file(new Path(inputPath, "PRAD-CA/sample.txt"))
        .id("CPCG0001-B1")
        .lineNumber(2)
        .build());
    list.add(Orphan.builder()
        .projectName("PRAD-CA")
        .fileType(FileType.SAMPLE_TYPE)
        .file(new Path(inputPath, "PRAD-CA/sample.txt"))
        .id("CPCG0001-F1")
        .lineNumber(3)
        .build());

    list.add(Orphan.builder()
        .projectName("LAML-KR")
        .fileType(FileType.DONOR_TYPE)
        .file(new Path(inputPath, "LAML-KR/donor.txt"))
        .id("Back_09")
        .lineNumber(60)
        .build());
    list.add(Orphan.builder()
        .projectName("LAML-KR")
        .fileType(FileType.DONOR_TYPE)
        .file(new Path(inputPath, "LAML-KR/donor.txt"))
        .id("SNU_NR_11")
        .lineNumber(73)
        .build());
    list.add(Orphan.builder()
        .projectName("LAML-KR")
        .fileType(FileType.SPECIMEN_TYPE)
        .file(new Path(inputPath, "LAML-KR/specimen.txt"))
        .id("Back_07_S1_sp")
        .lineNumber(113)
        .build());
    list.add(Orphan.builder()
        .projectName("LAML-KR")
        .fileType(FileType.SPECIMEN_TYPE)
        .file(new Path(inputPath, "LAML-KR/specimen.txt"))
        .id("SNU_NR_05_S1_sp")
        .lineNumber(129)
        .build());
    list.add(Orphan.builder()
        .projectName("LAML-KR")
        .fileType(FileType.SAMPLE_TYPE)
        .file(new Path(inputPath, "LAML-KR/sample.txt"))
        .id("Back-10-S1")
        .lineNumber(116)
        .build());
    list.add(Orphan.builder()
        .projectName("LAML-KR")
        .fileType(FileType.SAMPLE_TYPE)
        .file(new Path(inputPath, "LAML-KR/sample.txt"))
        .id("SNUH_G74_S1")
        .lineNumber(139)
        .build());

    list.add(Orphan.builder()
        .projectName("EOPC-DE")
        .fileType(FileType.DONOR_TYPE)
        .file(new Path(inputPath, "EOPC-DE/donor.txt"))
        .id("EOPC-N001")
        .lineNumber(13)
        .build());
    list.add(Orphan.builder()
        .projectName("EOPC-DE")
        .fileType(FileType.DONOR_TYPE)
        .file(new Path(inputPath, "EOPC-DE/donor.txt"))
        .id("EOPC-024")
        .lineNumber(17)
        .build());
    list.add(Orphan.builder()
        .projectName("EOPC-DE")
        .fileType(FileType.SPECIMEN_TYPE)
        .file(new Path(inputPath, "EOPC-DE/specimen.txt"))
        .id("EOPC-Normal1_RNA_pool_N001")
        .lineNumber(24)
        .build());
    list.add(Orphan.builder()
        .projectName("EOPC-DE")
        .fileType(FileType.SPECIMEN_TYPE)
        .file(new Path(inputPath, "EOPC-DE/specimen.txt"))
        .id("EOPC-022_tumor")
        .lineNumber(27)
        .build());
    list.add(Orphan.builder()
        .projectName("EOPC-DE")
        .fileType(FileType.SPECIMEN_TYPE)
        .file(new Path(inputPath, "EOPC-DE/specimen.txt"))
        .id("EOPC-026_normal")
        .lineNumber(34)
        .build());
    list.add(Orphan.builder()
        .projectName("EOPC-DE")
        .fileType(FileType.SAMPLE_TYPE)
        .file(new Path(inputPath, "EOPC-DE/sample.txt"))
        .id("EOPC-Normal1_RNA_pool_N001")
        .lineNumber(24)
        .build());
    list.add(Orphan.builder()
        .projectName("EOPC-DE")
        .fileType(FileType.SAMPLE_TYPE)
        .file(new Path(inputPath, "EOPC-DE/sample.txt"))
        .id("EOPC-018_normal")
        .lineNumber(30)
        .build());

    list.add(Orphan.builder()
        .projectName("ALL-US")
        .fileType(FileType.DONOR_TYPE)
        .file(new Path(inputPath, "ALL-US/donor.txt"))
        .id("TARGET-10-PAKFYA")
        .lineNumber(11)
        .build());
    list.add(Orphan.builder()
        .projectName("ALL-US")
        .fileType(FileType.DONOR_TYPE)
        .file(new Path(inputPath, "ALL-US/donor.txt"))
        .id("TARGET-10-PANSPW")
        .lineNumber(249)
        .build());
    list.add(Orphan.builder()
        .projectName("ALL-US")
        .fileType(FileType.SPECIMEN_TYPE)
        .file(new Path(inputPath, "ALL-US/specimen.txt"))
        .id("TARGET-10-PAMCMC-14A")
        .lineNumber(443)
        .build());
    list.add(Orphan.builder()
        .projectName("ALL-US")
        .fileType(FileType.SPECIMEN_TYPE)
        .file(new Path(inputPath, "ALL-US/specimen.txt"))
        .id("TARGET-10-PAPHJF-04A")
        .lineNumber(575)
        .build());
    list.add(Orphan.builder()
        .projectName("ALL-US")
        .fileType(FileType.SAMPLE_TYPE)
        .file(new Path(inputPath, "ALL-US/sample.txt"))
        .id("TARGET-10-PAPGYC-09A-01R")
        .lineNumber(730)
        .build());
    list.add(Orphan.builder()
        .projectName("ALL-US")
        .fileType(FileType.SAMPLE_TYPE)
        .file(new Path(inputPath, "ALL-US/sample.txt"))
        .id("TARGET-10-PARJSR-14A-01D")
        .lineNumber(1386)
        .build());

    return list;
  }
}
