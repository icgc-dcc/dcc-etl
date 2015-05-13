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
package org.icgc.dcc.etl.repo.pcawg.core;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.primitives.Longs.max;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static org.icgc.dcc.common.core.tcga.TCGAIdentifiers.isUUID;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.repo.model.RepositoryProjects.getProjectCodeProject;
import static org.icgc.dcc.etl.repo.model.RepositoryServers.getPCAWGServer;
import static org.icgc.dcc.etl.repo.pcawg.core.PCAWGFileDataTypeResolver.resolveFileDataTypes;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.PCAWG_LIBRARY_STRATEGY_NAMES;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.PCAWG_SPECIMEN_CLASSES;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.PCAWG_WORKFLOW_TYPES;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getBamFileMd5sum;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getBamFileName;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getBamFileSize;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getDccProjectCode;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getFileMd5sum;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getFileName;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getFileSize;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getFiles;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getGnosId;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getGnosLastModified;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getGnosRepo;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getSubmitterDonorId;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getSubmitterSampleId;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getSubmitterSpecimenId;

import java.time.Instant;
import java.util.Set;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.repo.core.RepositoryFileProcessor;
import org.icgc.dcc.etl.repo.model.RepositoryFile;
import org.icgc.dcc.etl.repo.model.RepositoryServers.RepositoryServer;

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
    for (val libraryStrategyName : PCAWG_LIBRARY_STRATEGY_NAMES) {
      val libraryStrategy = donor.path(libraryStrategyName);

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

            val analysisType = resolveAnalysisType(libraryStrategyName, specimenClass, workflowType);
            for (val workflowFile : getFiles(workflow)) {
              val donorFile = createDonorFile(projectCode, submittedDonorId, analysisType, workflow, workflowFile);

              donorFiles.add(donorFile);
            }
          }
        }
      }
    }

    return donorFiles.build();
  }

  private RepositoryFile createDonorFile(String projectCode, String submittedDonorId, String analysisType,
      JsonNode workflow,
      JsonNode workflowFile) {
    val project = getProjectCodeProject(projectCode).orNull();
    val pcawgServer = resolvePCAWGSerer(workflow);

    val submitterSpecimenId = getSubmitterSpecimenId(workflow);
    val submitterSampleId = getSubmitterSampleId(workflow);
    val fileName = resolveFileName(workflowFile);
    val fileSize = resolveFileSize(workflowFile);
    val dataTypes = resolveFileDataTypes(analysisType, fileName);

    val donorFile = new RepositoryFile();
    donorFile
        .setStudy("PCAWG")
        .setAccess("controlled");

    donorFile
        .getDataTypes().addAll(dataTypes);

    donorFile.getRepository()
        .setRepoType(pcawgServer.getType().getId())
        .setRepoOrg(pcawgServer.getSource().getId())
        .setRepoEntityId(getGnosId(workflow));

    donorFile.getRepository().getRepoServer().get(0)
        .setRepoName(pcawgServer.getName())
        .setRepoCode(pcawgServer.getCode())
        .setRepoCountry(pcawgServer.getCountry())
        .setRepoBaseUrl(pcawgServer.getBaseUrl());

    donorFile.getRepository()
        .setRepoMetadataPath(pcawgServer.getType().getMetadataPath())
        .setRepoDataPath(pcawgServer.getType().getDataPath())
        .setFileName(fileName)
        .setFileMd5sum(resolveMd5sum(workflowFile))
        .setFileSize(fileSize)
        .setLastModified(resolveLastModified(workflow));

    donorFile.getDonor()
        .setPrimarySite(resolvePrimarySite(projectCode))
        .setProgram(project.getProgram())
        .setProjectCode(projectCode)
        .setStudy(null)

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

  private static RepositoryServer resolvePCAWGSerer(JsonNode workflow) {
    val genosRepo = getGnosRepo(workflow);

    return getPCAWGServer(genosRepo);
  }

  private static String resolveAnalysisType(String libraryStrategyName, String specimenClass, String workflowType) {
    return libraryStrategyName + "." + specimenClass + "." + workflowType;
  }

  private static Set<String> resolveUUIDs(Iterable<RepositoryFile> donorFiles) {
    val uuids = Sets.<String> newHashSet();
    for (val donorFile : donorFiles) {
      val donorId = donorFile.getDonor().getSubmittedDonorId();
      val specimenId = donorFile.getDonor().getSubmittedSpecimenId();
      val sampleId = donorFile.getDonor().getSubmittedSampleId();

      // TODO: Check is TCGA?
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

  private static String resolveFileName(JsonNode workflowFile) {
    return firstNonNull(getFileName(workflowFile), getBamFileName(workflowFile));
  }

  private static String resolveMd5sum(JsonNode workflowFile) {
    return firstNonNull(getFileMd5sum(workflowFile), getBamFileMd5sum(workflowFile));
  }

  private static long resolveFileSize(JsonNode workflowFile) {
    // First non-zero
    return max(getFileSize(workflowFile), getBamFileSize(workflowFile));
  }

  private static String resolveLastModified(JsonNode workflow) {
    val text = getGnosLastModified(workflow);
    val dateTime = ISO_OFFSET_DATE_TIME.parse(text, Instant::from);

    return formatDateTime(dateTime);
  }

}
