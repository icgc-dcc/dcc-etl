/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.db.importer.repo.tcga.core;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubMetadata.CGHUB_BASE_URL;

import java.util.Map;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;
import org.icgc.dcc.etl.db.importer.repo.tcga.reader.TCGAArchiveListReader;

import com.google.common.collect.ImmutableList;

@Slf4j
@RequiredArgsConstructor
public class TCGAClinicalFileProcessor {

  @NonNull
  private final Map<String, String> primarySites;

  /**
   * Constants.
   */
  private static final Pattern CLINICAL_ARCHIVE_NAME_PATTERN = Pattern.compile(".*_(\\w+)\\.bio\\..*");

  public Iterable<RepositoryFile> process() {
    log.info("Reading archive list entries...");
    val entries = TCGAArchiveListReader.readEntries();
    log.info("Read {} archive list entries", formatCount(entries));

    val clinicalFiles = ImmutableList.<RepositoryFile> builder();
    for (val entry : entries) {
      val matcher = CLINICAL_ARCHIVE_NAME_PATTERN.matcher(entry.getArchiveName());
      val clinical = matcher.matches();
      if (!clinical) {
        continue;
      }

      val diseaseCode = matcher.group(1);
      val projectName = resolveProjectName(diseaseCode);
      val archiveClinicalFiles = processArchive(projectName, entry.getArchiveUrl());

      clinicalFiles.addAll(archiveClinicalFiles);
    }

    return clinicalFiles.build();
  }

  private Iterable<RepositoryFile> processArchive(String projectName, String archiveUrl) {
    val processor = new TCGAArchiveClinicalFileProcessor();
    val archiveClinicalFiles = processor.process(archiveUrl);
    log.info("Processing {} archive clinical files", formatCount(archiveClinicalFiles));

    val clinicalFiles = ImmutableList.<RepositoryFile> builder();
    for (val archiveClinicalFile : archiveClinicalFiles) {
      val tcgaFile = new RepositoryFile();
      tcgaFile.setStudy(null);
      tcgaFile.setAccess("open");

      tcgaFile.setDataType("Clinical");
      tcgaFile.setDataSubType("Clinical data");
      tcgaFile.setDataFormat("XML");

      tcgaFile.getRepository().setRepoType("TCGA");
      tcgaFile.getRepository().setRepoEntityId(null);

      tcgaFile.getRepository().getRepoServer().get(0).setRepoName("TCGA");
      tcgaFile.getRepository().getRepoServer().get(0).setRepoCountry("USA");
      tcgaFile.getRepository().getRepoServer().get(0).setRepoBaseUrl(CGHUB_BASE_URL);

      tcgaFile.getRepository().setRepoPath(archiveClinicalFile.getUrl());
      tcgaFile.getRepository().setFileName(archiveClinicalFile.getFileName());
      tcgaFile.getRepository().setFileMd5sum(archiveClinicalFile.getFileMd5());
      tcgaFile.getRepository().setFileSize(archiveClinicalFile.getFileSize());
      tcgaFile.getRepository().setLastModified(archiveClinicalFile.getLastModified().toString());

      tcgaFile.getDonor().setPrimarySite(primarySites.get(projectName));
      tcgaFile.getDonor().setProgram("TCGA");
      tcgaFile.getDonor().setProjectCode(projectName);

      tcgaFile.getDonor().setDonorId(null);
      tcgaFile.getDonor().setSpecimenId(null);
      tcgaFile.getDonor().setSampleId(null);

      tcgaFile.getDonor().setSubmittedDonorId(archiveClinicalFile.getDonorId());
      tcgaFile.getDonor().setSubmittedSpecimenId(null);
      tcgaFile.getDonor().setSubmittedSampleId(null);

      tcgaFile.getDonor().setTcgaParticipantBarcode(null);
      tcgaFile.getDonor().setTcgaSampleBarcode(null);
      tcgaFile.getDonor().setTcgaAliquotBarcode(null);

      clinicalFiles.add(tcgaFile);
    }

    return clinicalFiles.build();
  }

  private static String resolveProjectName(String diseaseCode) {
    return diseaseCode + "-US";
  }

}
