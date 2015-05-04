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
package org.icgc.dcc.etl.db.importer.pcawg.core;

import static org.icgc.dcc.etl.db.importer.pcawg.util.PCAWGArchives.PCAWG_DCC_PROJECT_CODE;
import static org.icgc.dcc.etl.db.importer.pcawg.util.PCAWGArchives.PCAWG_FILES_FIELD;
import static org.icgc.dcc.etl.db.importer.pcawg.util.PCAWGArchives.PCAWG_LIBRARY_STRATEGY_NAMES;
import static org.icgc.dcc.etl.db.importer.pcawg.util.PCAWGArchives.PCAWG_SPECIMEN_CLASSES;
import static org.icgc.dcc.etl.db.importer.pcawg.util.PCAWGArchives.PCAWG_SUBMITTER_DONOR_ID;
import static org.icgc.dcc.etl.db.importer.pcawg.util.PCAWGArchives.PCAWG_SUBMITTER_SAMPLE_ID;
import static org.icgc.dcc.etl.db.importer.pcawg.util.PCAWGArchives.PCAWG_SUBMITTER_SPECIMEN_ID;
import static org.icgc.dcc.etl.db.importer.pcawg.util.PCAWGArchives.PCAWG_WORKFLOW_TYPES;
import static org.icgc.dcc.etl.db.importer.repo.util.FileRepositories.formatDateTime;

import java.time.ZonedDateTime;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.db.importer.repo.model.FileRepositoryType;
import org.icgc.dcc.etl.db.importer.repo.util.FileRepositories;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class PCAWGDonorFilesConverter {

  /**
   * TODO: Get from documents
   */
  private static final ZonedDateTime LAST_UPDATED = ZonedDateTime.now();

  public Iterable<ObjectNode> convertDonor(@NonNull ObjectNode donor) {
    val submittedDonorId = donor.get(PCAWG_SUBMITTER_DONOR_ID);
    val projectId = donor.get(PCAWG_DCC_PROJECT_CODE);

    val files = ImmutableList.<ObjectNode> builder();
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

            val workflowFiles = convertWorkflowFiles((ObjectNode) workflow);
            for (val workflowFile : workflowFiles) {
              workflowFile.put(FileRepositories.FILE_REPOSITORY_TYPE_FIELD_NAME, FileRepositoryType.PCAWG.getId());
              workflowFile.put("_project_id", projectId);
              workflowFile.put("last_updated", formatDateTime(LAST_UPDATED));
              workflowFile.put("submitted_donor_id", submittedDonorId);
              workflowFile.put("specimen_class", normalizeSpecimenClass(specimenClass));
              workflowFile.put("workflow_type", workflowType);

              files.add(workflowFile);
            }
          }
        }
      }
    }

    return files.build();
  }

  private static String normalizeSpecimenClass(String specimenClass) {
    return specimenClass.toLowerCase().contains("normal") ? "normal" : "tumor";
  }

  private static Iterable<ObjectNode> convertWorkflowFiles(ObjectNode workflow) {
    val fileProperties = (ObjectNode) workflow.deepCopy().without(PCAWG_FILES_FIELD);
    val workflowFiles = workflow.path(PCAWG_FILES_FIELD);

    val files = ImmutableList.<ObjectNode> builder();
    for (val workflowFile : workflowFiles) {
      val file = fileProperties.deepCopy();
      rename(file, PCAWG_SUBMITTER_SAMPLE_ID, "submitted_sample_id");
      rename(file, PCAWG_SUBMITTER_SPECIMEN_ID, "submitted_specimen_id");

      file.putAll((ObjectNode) workflowFile);

      files.add(file);
    }

    return files.build();
  }

  @NonNull
  private static void rename(ObjectNode objectNode, String fromFieldName, String toFieldName) {
    objectNode.put(toFieldName, objectNode.remove(fromFieldName));
  }

}
