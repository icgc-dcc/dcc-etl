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
import static org.icgc.dcc.common.core.tcga.TCGAIdentifiers.isUUID;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.repo.model.RepositoryProjects.getProjectCodeProject;
import static org.icgc.dcc.etl.repo.model.RepositoryServers.getPCAWGServer;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.PCAWG_LIBRARY_STRATEGY_NAMES;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.PCAWG_SPECIMEN_CLASSES;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.PCAWG_WORKFLOW_TYPES;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getBamFileName;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getBamFileSize;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getDccProjectCode;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getFileName;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getFileSize;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getFiles;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getGnosId;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getGnosRepo;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getSubmitterDonorId;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getSubmitterSampleId;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.getSubmitterSpecimenId;

import java.time.ZonedDateTime;
import java.util.Set;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.repo.core.RepositoryFileProcessor;
import org.icgc.dcc.etl.repo.model.RepositoryFile;
import org.icgc.dcc.etl.repo.model.RepositoryFileDataType;

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

            val analysisType = name + "." + specimenClass + "." + workflowType;
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
    val genosRepo = getGnosRepo(workflow);
    val pcawgServer = getPCAWGServer(genosRepo);

    val submitterSpecimenId = getSubmitterSpecimenId(workflow);
    val submitterSampleId = getSubmitterSampleId(workflow);
    val fileName = resolveFileName(workflowFile);
    val fileSize = resolveFileSize(workflowFile);
    val dataType = resolveDataType(analysisType, fileName);

    val donorFile = new RepositoryFile();
    donorFile
        .setStudy("PCAWG")

        .setAccess(dataType.getAccess())
        .setExperimentalStrategy(dataType.getExperimentalStrategy())
        .setDataType(dataType.getDataType())
        .setDataFormat(dataType.getDataFormat());

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

  private RepositoryFileDataType resolveDataType(String analysisType, String fileName) {
    String dataType = null;
    String dataFormat = null;
    String experimentalStrategy = null;

    if (analysisType.matches("rna_seq\\..*\\.(star|tophat)")) {
      dataType = "RNA-Seq";
      dataFormat = "BAM";
      experimentalStrategy = "RNA-Seq";
    } else if (analysisType.matches("wgs\\..*\\.bwa_alignment")) {
      dataType = "DNA-Seq";
      dataFormat = "BAM";
      experimentalStrategy = "RNA-Seq";
    } else if (analysisType.matches("wgs\\.tumor_specimens\\.sanger_variant_calling")) {
      dataFormat = "VCF";
      experimentalStrategy = "WGS";

      if (fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
        dataType = "Somatic SNV MNV";
      } else if (fileName.endsWith(".somatic.cnv.vcf.gz")) {
        dataType = "Somatic CNV";
      } else if (fileName.endsWith(".somatic.sv.vcf.gz")) {
        dataType = "Somatic SV";
      } else if (fileName.endsWith(".somatic.indel.vcf.gz")) {
        dataType = "Somatic Indel";
      } else if (fileName.endsWith(".somatic.snv_mnv.tar.gz")) {
        dataType = "Somatic SNV MNV";
      } else if (fileName.endsWith(".somatic.sv.tar.gz")) {
        dataType = "Somatic SV";
      } else if (fileName.endsWith(".somatic.indel.tar.gz")) {
        dataType = "Somatic Indel";
      } else if (fileName.endsWith(".somatic.imputeCounts.tar.gz")) {
        dataType = "Somatic Impute Counts";
      } else if (fileName.endsWith(".somatic.binnedReadCounts.tar.gz")) {
        dataType = "Somatic Binned Read Counts";
      } else if (fileName.endsWith(".somatic.genotype.tar.gz")) {
        dataType = "Somatic Genotype";
      } else if (fileName.endsWith(".somatic.verifyBamId.tar.gz")) {
        dataType = "Somatic Verify BAM Id";
      }
    }

    return new RepositoryFileDataType()
        .setDataType(dataType)
        .setDataFormat(dataFormat)
        .setExperimentalStrategy(experimentalStrategy)
        .setAccess("controlled");
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
