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
import static org.elasticsearch.common.primitives.Longs.max;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_FILES_FIELD;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_LIBRARY_STRATEGY_NAMES;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_SPECIMEN_CLASSES;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_WORKFLOW_TYPES;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getBamFileName;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getBamFileSize;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getDccProjectCode;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getFileName;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getFileSize;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getGnosId;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getGnosRepo;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getSubmitterDonorId;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getSubmitterSampleId;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.getSubmitterSpecimenId;
import static org.icgc.dcc.etl.db.importer.repo.util.FileRepositories.formatDateTime;

import java.time.ZonedDateTime;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@RequiredArgsConstructor
public class PCAWGProcessor {

  /**
   * TODO: Get from documents
   */
  private static final ZonedDateTime LAST_UPDATED = ZonedDateTime.now();

  /**
   * Metadata.
   */
  @NonNull
  private final Map<String, String> primarySites;

  public Iterable<RepositoryFile> processFiles(@NonNull Iterable<ObjectNode> donors) {

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

            for (val workflowFile : workflow.path(PCAWG_FILES_FIELD)) {
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
    val fileName = resolveFileName(workflowFile);
    val fileSize = resolveFileSize(workflowFile);

    val donorFile = new RepositoryFile();
    donorFile.setStudy("PCAWG");
    donorFile.setAccess("controlled");

    donorFile.setDataType(null);
    donorFile.setDataSubType(null);
    donorFile.setDataFormat(null);

    donorFile.getRepository().setRepoType("GNOS");
    donorFile.getRepository().setRepoOrg("PCAWG");
    donorFile.getRepository().setRepoEntityId(getGnosId(workflow));

    donorFile.getRepository().getRepoServer().get(0).setRepoName("PCAWG");
    donorFile.getRepository().getRepoServer().get(0).setRepoCountry("USA");
    donorFile.getRepository().getRepoServer().get(0).setRepoBaseUrl(getGnosRepo(workflow));

    donorFile.getRepository().setRepoPath(null);
    donorFile.getRepository().setFileName(fileName);
    donorFile.getRepository().setFileMd5sum(null);
    donorFile.getRepository().setFileSize(fileSize);
    donorFile.getRepository().setLastModified(formatDateTime(LAST_UPDATED));

    donorFile.getDonor().setPrimarySite(resolvePrimarySite(projectCode));
    donorFile.getDonor().setProgram(null);
    donorFile.getDonor().setProjectCode(projectCode);

    donorFile.getDonor().setDonorId(null);
    donorFile.getDonor().setSpecimenId(null);
    donorFile.getDonor().setSampleId(null);

    donorFile.getDonor().setSubmittedDonorId(submittedDonorId);
    donorFile.getDonor().setSubmittedSpecimenId(getSubmitterSpecimenId(workflow));
    donorFile.getDonor().setSubmittedSampleId(getSubmitterSampleId(workflow));

    donorFile.getDonor().setTcgaParticipantBarcode(null);
    donorFile.getDonor().setTcgaSampleBarcode(null);
    donorFile.getDonor().setTcgaAliquotBarcode(null);

    return donorFile;
  }

  private String resolvePrimarySite(String projectCode) {
    return primarySites.get(projectCode);
  }

  private static String resolveFileName(JsonNode workflowFile) {
    return firstNonNull(getFileName(workflowFile), getBamFileName(workflowFile));
  }

  private static long resolveFileSize(JsonNode workflowFile) {
    return max(getFileSize(workflowFile), getBamFileSize(workflowFile));
  }

}
