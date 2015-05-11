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
package org.icgc.dcc.etl.db.importer.repo.pcawg.core;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.primitives.Longs.max;
import static org.icgc.dcc.common.core.tcga.TCGAIdentifiers.isUUID;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.db.importer.repo.model.RepositoryProjects.getProjectCodeProject;
import static org.icgc.dcc.etl.db.importer.repo.model.RepositoryServers.getPCAWGServer;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_LIBRARY_STRATEGY_NAMES;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_SPECIMEN_CLASSES;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_WORKFLOW_TYPES;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getBamFileName;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getBamFileSize;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getDccProjectCode;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getFileName;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getFileSize;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getFiles;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getGnosId;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getGnosRepo;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getSubmitterDonorId;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getSubmitterSampleId;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getSubmitterSpecimenId;

import java.time.ZonedDateTime;
import java.util.Set;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.db.importer.repo.core.RepositoryFileProcessor;
import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

@Slf4j
public class PCAWGDonorProcessor extends RepositoryFileProcessor {

  public PCAWGDonorProcessor(RepositoryFileContext context) {
    super(context);
  }

  public Iterable<RepositoryFile> processDonors(@NonNull Iterable<ObjectNode> donors) {
    val donorFiles = createDonorFiles(donors);

    // The business
    translateUUIDs(donorFiles);
    assignIds(donorFiles);

    return donorFiles;
  }

  private void translateUUIDs(Iterable<RepositoryFile> donorFiles) {
    log.info("Collecting TCGA barcodes...");
    val uuids = resolveUUIDs(donorFiles);

    log.info("Translating {} TCGA barcodes to TCGA UUIDs...", formatCount(uuids));
    val barcodes = resolveTCGABarcodes(uuids);
    for (val donorFile : donorFiles) {
      val submittedDonorId = donorFile.getDonor().getSubmittedDonorId();
      val submittedSpecimenId = donorFile.getDonor().getSubmittedSpecimenId();
      val submittedSampleId = donorFile.getDonor().getSubmittedSampleId();

      val tcgaParticipantBarcode = barcodes.get(submittedDonorId);
      val tcgaSampleBarcode = barcodes.get(submittedSpecimenId);
      val tcgaAliquotBarcode = barcodes.get(submittedSampleId);

      donorFile.getDonor()
          .setTcgaParticipantBarcode(tcgaParticipantBarcode)
          .setTcgaSampleBarcode(tcgaSampleBarcode)
          .setTcgaAliquotBarcode(tcgaAliquotBarcode);
    }
  }

  private void assignIds(Iterable<RepositoryFile> donorFiles) {
    log.info("Assigning ICGC ids...");
    for (val donorFile : donorFiles) {
      val projectCode = donorFile.getDonor().getProjectCode();
      val submittedDonorId = donorFile.getDonor().getSubmittedDonorId();
      val submittedSpecimenId = donorFile.getDonor().getSubmittedSpecimenId();
      val submittedSampleId = donorFile.getDonor().getSubmittedSampleId();

      donorFile.getDonor()
          .setDonorId(resolveDonorId(projectCode, submittedDonorId))
          .setSpecimenId(resolveSpecimenId(projectCode, submittedSpecimenId))
          .setSampleId(resolveSampleId(projectCode, submittedSampleId));
    }
  }

  private Iterable<RepositoryFile> createDonorFiles(Iterable<ObjectNode> donors) {
    val files = ImmutableList.<RepositoryFile> builder();
    for (val donor : donors) {
      val donorFiles = processDonor(donor);

      files.addAll(donorFiles);
    }

    return files.build();
  }

  private Iterable<RepositoryFile> processDonor(@NonNull ObjectNode donor) {
    val projectCode = getDccProjectCode(donor);
    val submittedDonorId = getSubmitterDonorId(donor);

    val donorFiles = ImmutableList.<RepositoryFile> builder();
    for (val name : PCAWG_LIBRARY_STRATEGY_NAMES) {
      val libraryStrategy = donor.path(name);

      if (libraryStrategy.isMissingNode()) {
        continue;
      }

      for (val specimenClass : PCAWG_SPECIMEN_CLASSES) {
        val specimens = libraryStrategy.path(specimenClass);
        for (val specimen : specimens) {
          for (val workflowType : PCAWG_WORKFLOW_TYPES) {
            val workflow = specimen.path(workflowType);
            if (workflow.isMissingNode()) {
              continue;
            }

            for (val workflowFile : getFiles(workflow)) {
              val donorFile = createDonorFile(projectCode, submittedDonorId, workflow, workflowFile);

              donorFiles.add(donorFile);
            }
          }
        }
      }
    }

    return donorFiles.build();
  }

  private RepositoryFile createDonorFile(String projectCode, String submittedDonorId, JsonNode workflow,
      JsonNode workflowFile) {
    val project = getProjectCodeProject(projectCode).orNull();
    val genosRepo = getGnosRepo(workflow);
    val server = getPCAWGServer(genosRepo);

    val submitterSpecimenId = getSubmitterSpecimenId(workflow);
    val submitterSampleId = getSubmitterSampleId(workflow);
    val fileName = resolveFileName(workflowFile);
    val fileSize = resolveFileSize(workflowFile);

    val donorFile = new RepositoryFile();
    donorFile
        .setStudy("PCAWG")
        .setAccess(resolveAccess(fileName))

        .setDataType(null)
        .setDataSubType(null)
        .setDataFormat(null);

    donorFile.getRepository()
        .setRepoType(server.getType().getId())
        .setRepoOrg(server.getOrg().getId())
        .setRepoEntityId(getGnosId(workflow));

    donorFile.getRepository().getRepoServer().get(0)
        .setRepoName(server.getName())
        .setRepoCountry(server.getCountry())
        .setRepoBaseUrl(server.getBaseUrl());

    donorFile.getRepository()
        .setRepoPath(server.getType().getPath())
        .setFileName(fileName)
        .setFileMd5sum(null)
        .setFileSize(fileSize)
        .setLastModified(resolveLastModified(workflow));

    donorFile.getDonor()
        .setPrimarySite(resolvePrimarySite(projectCode))
        .setProgram(project.getProgram())
        .setProjectCode(projectCode)

        .setDonorId(null)
        .setSpecimenId(null)
        .setSampleId(null)

        .setSubmittedDonorId(submittedDonorId)
        .setSubmittedSpecimenId(submitterSpecimenId)
        .setSubmittedSampleId(submitterSampleId)

        .setTcgaParticipantBarcode(null)
        .setTcgaSampleBarcode(null)
        .setTcgaAliquotBarcode(null);

    return donorFile;
  }

  private static Set<String> resolveUUIDs(Iterable<RepositoryFile> donorFiles) {
    val uuids = Sets.<String> newHashSet();
    for (val donorFile : donorFiles) {
      val donorId = donorFile.getDonor().getSubmittedDonorId();
      val specimenId = donorFile.getDonor().getSubmittedSpecimenId();
      val sampleId = donorFile.getDonor().getSubmittedSampleId();

      if (isUUID(donorId)) {
        uuids.add(donorId);
      }
      if (isUUID(specimenId)) {
        uuids.add(specimenId);
      }
      if (isUUID(sampleId)) {
        uuids.add(sampleId);
      }
    }
    return uuids;
  }

  private static String resolveAccess(String fileName) {
    // TODO: mapping
    return "controlled";
  }

  private static String resolveFileName(JsonNode workflowFile) {
    return firstNonNull(getFileName(workflowFile), getBamFileName(workflowFile));
  }

  private static long resolveFileSize(JsonNode workflowFile) {
    // First non-zero
    return max(getFileSize(workflowFile), getBamFileSize(workflowFile));
  }

  private static String resolveLastModified(JsonNode workflow) {
    // TODO: mapping
    return formatDateTime(ZonedDateTime.now());
  }

}
