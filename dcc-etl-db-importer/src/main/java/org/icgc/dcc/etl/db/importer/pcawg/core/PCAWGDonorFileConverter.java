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

import java.util.List;

import lombok.NonNull;
import lombok.val;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class PCAWGDonorFileConverter {

  /**
   * Constants.
   */
  private static final String SUBMITTER_DONOR_ID = "submitter_donor_id";
  private static final List<String> SPECIMEN_CLASSES = ImmutableList.of("normal_specimen", "normal_specimens",
      "tumor_specimen", "tumor_specimens");
  private static final List<String> WORKFLOW_TYPES = ImmutableList.of("star", "tophat", "bwa_alignment",
      "sanger_variant_calling");
  private static final List<String> LIBRARY_STRATEGY_NAMES = ImmutableList.of("rna_seq", "wgs");

  @NonNull
  public Iterable<ObjectNode> convert(ObjectNode donor) {
    val submittedDonorId = donor.get(SUBMITTER_DONOR_ID);
    val projectName = donor.get("dcc_project_code");

    val files = ImmutableList.<ObjectNode> builder();
    for (val name : LIBRARY_STRATEGY_NAMES) {
      val libraryStrategy = donor.path(name);

      if (libraryStrategy.isObject()) {
        for (val specimenClass : SPECIMEN_CLASSES) {
          val specimens = libraryStrategy.path(specimenClass);
          for (val specimen : specimens) {
            for (val workflowType : WORKFLOW_TYPES) {
              val workflow = specimen.path(workflowType);
              if (workflow.isMissingNode()) {
                continue;
              }

              for (val file : convertWorkflowFiles((ObjectNode) workflow)) {
                file.put("project_name", projectName);
                file.put("submitted_donor_id", submittedDonorId);
                file.put("specimen_class", normalizeSpecimenClass(specimenClass));
                file.put("workflow_type", workflowType);

                files.add(file);
              }
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
    val fileProperties = (ObjectNode) workflow.deepCopy().without("files");

    val files = ImmutableList.<ObjectNode> builder();
    for (val workflowFile : workflow.path("files")) {
      val file = fileProperties.deepCopy();
      file.putAll((ObjectNode) workflowFile);

      files.add(file);
    }

    return files.build();
  }

}
