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
package org.icgc.dcc.etl.repo.tcga.core;

import static com.google.common.collect.Iterables.filter;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.etl.repo.model.RepositoryProjects.getDiseaseCodeProject;
import static org.icgc.dcc.etl.repo.model.RepositoryServers.getTCGAServer;

import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.repo.core.RepositoryFileProcessor;
import org.icgc.dcc.etl.repo.model.RepositoryFile;
import org.icgc.dcc.etl.repo.model.RepositoryServers.RepositoryServer;
import org.icgc.dcc.etl.repo.tcga.model.TCGAArchiveClinicalFile;
import org.icgc.dcc.etl.repo.tcga.reader.TCGAArchiveListReader;

import com.google.common.collect.ImmutableList;

@Slf4j
public class TCGAClinicalFileProcessor extends RepositoryFileProcessor {

  /**
   * Constants.
   */
  private static final Pattern CLINICAL_ARCHIVE_NAME_PATTERN = Pattern.compile(".*_(\\w+)\\.bio\\..*");

  /**
   * Metadata.
   */
  @NonNull
  private final RepositoryServer tcgaServer = getTCGAServer();

  public TCGAClinicalFileProcessor(RepositoryFileContext context) {
    super(context);
  }

  public Iterable<RepositoryFile> processClinicalFiles() {
    // Key steps, order matters
    val clinicalFiles = createClinicalFiles();
    val filteredClinicalFiles = filterClinicalFiles(clinicalFiles);
    translateBarcodes(filteredClinicalFiles);
    assignStudy(filteredClinicalFiles);

    return filteredClinicalFiles;
  }

  private Iterable<RepositoryFile> filterClinicalFiles(Iterable<RepositoryFile> clinicalFiles) {
    return filter(clinicalFiles, clinicalFile -> clinicalFile.getDonor().hasDonorId());
  }

  private Iterable<RepositoryFile> createClinicalFiles() {
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
      val project = getDiseaseCodeProject(diseaseCode);
      val unrecognized = !project.isPresent();
      if (unrecognized) {
        continue;
      }

      val archiveClinicalFiles = processArchive(project.get().getProjectCode(), entry.getArchiveUrl());

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
      val clinicalFile = createClinicalFile(projectCode, archiveClinicalFile);

      clinicalFiles.add(clinicalFile);
    }

    return clinicalFiles.build();
  }

  private RepositoryFile createClinicalFile(String projectCode, TCGAArchiveClinicalFile archiveClinicalFile) {
    val submittedDonorId = archiveClinicalFile.getDonorId();

    val repoEntityId = resolveRepoEntityId(archiveClinicalFile);

    val clinicalFile = new RepositoryFile()
        .setId(resolveFileId(tcgaServer.getType().getDataPath(), repoEntityId))
        .setStudy(null) // N/A
        .setAccess("open");

    clinicalFile.getDataType()
        .setDataType("Clinical")
        .setDataFormat("XML")
        .setExperimentalStrategy(null); // N/A

    clinicalFile.getRepository()
        .setRepoType(tcgaServer.getType().getId())
        .setRepoOrg(tcgaServer.getSource().getId())
        .setRepoEntityId(repoEntityId);

    clinicalFile.getRepository().getRepoServer().get(0)
        .setRepoName(tcgaServer.getName())
        .setRepoCode(tcgaServer.getCode())
        .setRepoCountry(tcgaServer.getCountry())
        .setRepoBaseUrl(tcgaServer.getBaseUrl());

    clinicalFile.getRepository()
        .setRepoMetadataPath(tcgaServer.getType().getMetadataPath())
        .setRepoDataPath(tcgaServer.getType().getDataPath())
        .setFileName(archiveClinicalFile.getFileName())
        .setFileMd5sum(archiveClinicalFile.getFileMd5())
        .setFileSize(archiveClinicalFile.getFileSize())
        .setLastModified(archiveClinicalFile.getLastModified().toString());

    clinicalFile.getDonor()
        .setPrimarySite(context.getPrimarySite(projectCode))
        .setProgram("TCGA")
        .setProjectCode(projectCode)

        .setStudy(null) // Set downstream

        .setDonorId(context.getDonorId(submittedDonorId, projectCode))
        .setSpecimenId(null) // N/A
        .setSampleId(null) // N/A

        .setSubmittedDonorId(null) // Set downstream
        .setSubmittedSpecimenId(null) // Set downstream
        .setSubmittedSampleId(null) // Set downstream

        .setTcgaParticipantBarcode(submittedDonorId)
        .setTcgaSampleBarcode(null) // N/A
        .setTcgaAliquotBarcode(null); // N/A

    return clinicalFile;
  }

  private void translateBarcodes(Iterable<RepositoryFile> clinicalFiles) {
    log.info("Collecting TCGA barcodes...");
    val barcodes = stream(clinicalFiles)
        .map(clinicalFile -> clinicalFile.getDonor().getTcgaParticipantBarcode())
        .collect(toImmutableSet());

    log.info("Translating {} TCGA barcodes to TCGA UUIDs...", formatCount(barcodes));

    val uuids = context.getTCGAUUIDs(barcodes);
    for (val clinicalFile : clinicalFiles) {
      val participantBarcode = clinicalFile.getDonor().getTcgaParticipantBarcode();

      val uuid = uuids.get(participantBarcode);
      clinicalFile.getDonor().setSubmittedDonorId(uuid);
    }
  }

  private void assignStudy(Iterable<RepositoryFile> clinicalFiles) {
    for (val clinicalFile : clinicalFiles) {
      val donor = clinicalFile.getDonor();
      val pcawg = context.isPCAWGSubmittedDonorId(donor.getProjectCode(), donor.getSubmittedDonorId());
      if (pcawg) {
        donor.setStudy(PCAWG_STUDY_VALUE);
      }
    }
  }

  private String resolveRepoEntityId(TCGAArchiveClinicalFile archiveClinicalFile) {
    val url = archiveClinicalFile.getUrl();
    return url.replace(tcgaServer.getBaseUrl(), "/").replace(tcgaServer.getType().getDataPath(), "");
  }

}
