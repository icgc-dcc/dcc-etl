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

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.core.id.IdentifierClient;
import org.icgc.dcc.etl.db.importer.repo.core.RepositoryTypeProcessor;
import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;
import org.icgc.dcc.etl.db.importer.repo.tcga.reader.TCGAArchiveListReader;
import org.icgc.dcc.etl.db.importer.util.UUID5;

import com.google.common.collect.ImmutableList;

@Slf4j
public class TCGAClinicalFileProcessor extends RepositoryTypeProcessor {

  public TCGAClinicalFileProcessor(Map<String, String> primarySites, IdentifierClient identifierClient) {
    super(primarySites, identifierClient);
  }

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

  private Iterable<RepositoryFile> processArchive(String projectCode, String archiveUrl) {
    val processor = new TCGAArchiveClinicalFileProcessor();
    val archiveClinicalFiles = processor.process(archiveUrl);
    log.info("Processing {} archive clinical files", formatCount(archiveClinicalFiles));

    val clinicalFiles = ImmutableList.<RepositoryFile> builder();
    for (val archiveClinicalFile : archiveClinicalFiles) {
      val submittedDonorId = archiveClinicalFile.getDonorId();

      val clinicalFile = new RepositoryFile();
      clinicalFile.setStudy(null);
      clinicalFile.setAccess("open");

      clinicalFile.setDataType("Clinical");
      clinicalFile.setDataSubType("Clinical data");
      clinicalFile.setDataFormat("XML");

      clinicalFile.getRepository().setRepoType("TCGA");
      clinicalFile.getRepository().setRepoOrg("TCGA");
      clinicalFile.getRepository().setRepoEntityId(UUID5.fromUTF8(archiveClinicalFile.getUrl()).toString());

      clinicalFile.getRepository().getRepoServer().get(0).setRepoName("TCGA");
      clinicalFile.getRepository().getRepoServer().get(0).setRepoCountry("USA");
      clinicalFile.getRepository().getRepoServer().get(0).setRepoBaseUrl(CGHUB_BASE_URL);

      clinicalFile.getRepository().setRepoPath(archiveClinicalFile.getUrl());
      clinicalFile.getRepository().setFileName(archiveClinicalFile.getFileName());
      clinicalFile.getRepository().setFileMd5sum(archiveClinicalFile.getFileMd5());
      clinicalFile.getRepository().setFileSize(archiveClinicalFile.getFileSize());
      clinicalFile.getRepository().setLastModified(archiveClinicalFile.getLastModified().toString());

      clinicalFile.getDonor().setPrimarySite(resolvePrimarySite(projectCode));
      clinicalFile.getDonor().setProgram("TCGA");
      clinicalFile.getDonor().setProjectCode(projectCode);

      clinicalFile.getDonor().setDonorId(resolveDonorId(projectCode, submittedDonorId));
      clinicalFile.getDonor().setSpecimenId(null);
      clinicalFile.getDonor().setSampleId(null);

      clinicalFile.getDonor().setSubmittedDonorId(submittedDonorId);
      clinicalFile.getDonor().setSubmittedSpecimenId(null);
      clinicalFile.getDonor().setSubmittedSampleId(null);

      clinicalFile.getDonor().setTcgaParticipantBarcode(null);
      clinicalFile.getDonor().setTcgaSampleBarcode(null);
      clinicalFile.getDonor().setTcgaAliquotBarcode(null);

      clinicalFiles.add(clinicalFile);
    }

    return clinicalFiles.build();
  }

  private static String resolveProjectName(String diseaseCode) {
    return diseaseCode + "-US";
  }

}
