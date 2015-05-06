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

import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_DCC_PROJECT_CODE;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_FILES_FIELD;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_LIBRARY_STRATEGY_NAMES;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_SPECIMEN_CLASSES;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_SUBMITTER_DONOR_ID;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_SUBMITTER_SAMPLE_ID;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_SUBMITTER_SPECIMEN_ID;
import static org.icgc.dcc.etl.db.importer.repo.pcawg.util.PCAWGArchives.PCAWG_WORKFLOW_TYPES;
import static org.icgc.dcc.etl.db.importer.repo.util.FileRepositories.formatDateTime;

import java.time.ZonedDateTime;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;

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
    val submittedDonorId = donor.get(PCAWG_SUBMITTER_DONOR_ID);
    val projectId = donor.get(PCAWG_DCC_PROJECT_CODE);

    val files = ImmutableList.<RepositoryFile> builder();
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
              val projectCode = projectId.textValue();

              String fileName = null;
              if (workflowFile.has("file_name")) {
                fileName = workflowFile.path("file_name").textValue();
              }
              if (workflowFile.has("bam_file_name")) {
                fileName = workflowFile.path("bam_file_name").textValue();
              }

              Long fileSize = null;
              if (workflowFile.has("file_size")) {
                fileSize = workflowFile.path("file_size").longValue();
              }
              if (workflowFile.has("bam_file_size")) {
                fileSize = workflowFile.path("bam_file_size").longValue();
              }

              val pcawgFile = new RepositoryFile();
              pcawgFile.setStudy("PCAWG");
              pcawgFile.setAccess("controlled");

              pcawgFile.setDataType(null);
              pcawgFile.setDataSubType(null);
              pcawgFile.setDataFormat(null);

              pcawgFile.getRepository().setRepoType("PCAWG");
              pcawgFile.getRepository().setRepoEntityId(workflow.get("gnos_id").textValue());

              pcawgFile.getRepository().getRepoServer().get(0).setRepoName("PCAWG");
              pcawgFile.getRepository().getRepoServer().get(0).setRepoCountry("USA");
              pcawgFile.getRepository().getRepoServer().get(0)
                  .setRepoBaseUrl(workflow.get("gnos_repo").get(0).textValue());

              pcawgFile.getRepository().setRepoPath(null);
              pcawgFile.getRepository().setFileName(fileName);
              pcawgFile.getRepository().setFileMd5sum(null);
              pcawgFile.getRepository().setFileSize(fileSize);
              pcawgFile.getRepository().setLastModified(formatDateTime(LAST_UPDATED));

              pcawgFile.getDonor().setPrimarySite(primarySites.get(projectCode));
              pcawgFile.getDonor().setProgram(null);
              pcawgFile.getDonor().setProjectCode(projectCode);

              pcawgFile.getDonor().setDonorId(null);
              pcawgFile.getDonor().setSpecimenId(null);
              pcawgFile.getDonor().setSampleId(null);

              pcawgFile.getDonor().setSubmittedDonorId(submittedDonorId.textValue());
              pcawgFile.getDonor().setSubmittedSpecimenId(workflow.get(PCAWG_SUBMITTER_SPECIMEN_ID).textValue());
              pcawgFile.getDonor().setSubmittedSampleId(workflow.get(PCAWG_SUBMITTER_SAMPLE_ID).textValue());

              pcawgFile.getDonor().setTcgaParticipantBarcode(null);
              pcawgFile.getDonor().setTcgaSampleBarcode(null);
              pcawgFile.getDonor().setTcgaAliquotBarcode(null);

              files.add(pcawgFile);
            }
          }
        }
      }
    }

    return files.build();
  }

}
