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

import static com.google.common.collect.Iterables.isEmpty;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.db.importer.repo.model.RepositoryServers.getTCGAServer;

import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.common.collect.Sets;
import org.icgc.dcc.etl.db.importer.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.db.importer.repo.core.RepositoryFileProcessor;
import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;
import org.icgc.dcc.etl.db.importer.repo.model.RepositoryServers.RepositoryServer;
import org.icgc.dcc.etl.db.importer.repo.tcga.reader.TCGAArchiveListReader;
import org.icgc.dcc.etl.db.importer.util.UUID5;

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
  private final RepositoryServer server = getTCGAServer();

  public TCGAClinicalFileProcessor(RepositoryFileContext context) {
    super(context);
  }

  public Iterable<RepositoryFile> processClinicalFiles() {
    val clinicalFiles = createClinicalFiles();

    if (isEmpty(clinicalFiles)) {
      // TODO: Use previous days values
      log.warn("**** TCGA site is down! Returing empty list");
      return ImmutableList.of();
    }

    translateBarcodes(clinicalFiles);

    return clinicalFiles;
  }

  private void translateBarcodes(Iterable<RepositoryFile> clinicalFiles) {
    log.info("Collecting barcodes...");
    val barcodes = Sets.<String> newHashSet();
    for (val clinicalFile : clinicalFiles) {
      val participantBarcode = clinicalFile.getDonor().getTcgaParticipantBarcode();

      barcodes.add(participantBarcode);
    }

    log.info("Translating barcodes to UUIDs...");
    val uuids = resolveTCGAUUIDs(barcodes);
    for (val clinicalFile : clinicalFiles) {
      val participantBarcode = clinicalFile.getDonor().getTcgaParticipantBarcode();

      val uuid = uuids.get(participantBarcode);
      clinicalFile.getDonor().setSubmittedDonorId(uuid);
    }
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

      clinicalFile.getRepository().setRepoType(server.getType().getId());
      clinicalFile.getRepository().setRepoOrg(server.getOrg().getId());
      clinicalFile.getRepository().setRepoEntityId(UUID5.fromUTF8(archiveClinicalFile.getUrl()).toString());

      clinicalFile.getRepository().getRepoServer().get(0).setRepoName(server.getName());
      clinicalFile.getRepository().getRepoServer().get(0).setRepoCountry(server.getCountry());
      clinicalFile.getRepository().getRepoServer().get(0).setRepoBaseUrl(server.getBaseUrl());

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

      clinicalFile.getDonor().setSubmittedDonorId(null);
      clinicalFile.getDonor().setSubmittedSpecimenId(null);
      clinicalFile.getDonor().setSubmittedSampleId(null);

      clinicalFile.getDonor().setTcgaParticipantBarcode(submittedDonorId);
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
