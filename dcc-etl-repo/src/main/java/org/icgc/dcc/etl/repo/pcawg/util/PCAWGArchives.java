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
package org.icgc.dcc.etl.repo.pcawg.util;

import static lombok.AccessLevel.PRIVATE;
import static org.elasticsearch.common.base.Strings.emptyToNull;

import java.util.List;

import lombok.NoArgsConstructor;
import lombok.NonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@NoArgsConstructor(access = PRIVATE)
public class PCAWGArchives {

  /**
   * URLs.
   */
  public static final String PCAWG_ARCHIVE_BASE_URL = "http://pancancer.info";

  /**
   * Field values.
   */
  public static final List<String> PCAWG_LIBRARY_STRATEGY_NAMES = ImmutableList.of(
      "rna_seq", "wgs");
  public static final List<String> PCAWG_SPECIMEN_CLASSES = ImmutableList.of(
      "normal_specimen", "normal_specimens", "tumor_specimen", "tumor_specimens");
  public static final List<String> PCAWG_WORKFLOW_TYPES = ImmutableList.of(
      "star", "tophat", "bwa_alignment", "sanger_variant_calling");

  public static String getDccProjectCode(@NonNull ObjectNode donor) {
    return donor.get("dcc_project_code").textValue();
  }

  public static String getFileName(@NonNull JsonNode workflowFile) {
    return emptyToNull(workflowFile.path("file_name").asText());
  }

  public static long getFileSize(@NonNull JsonNode workflowFile) {
    return workflowFile.path("file_size").asLong();
  }

  public static String getFileMd5sum(@NonNull JsonNode workflowFile) {
    return emptyToNull(workflowFile.path("file_md5sum").asText());
  }

  public static String getBamFileName(@NonNull JsonNode workflowFile) {
    return emptyToNull(workflowFile.path("bam_file_name").asText());
  }

  public static long getBamFileSize(@NonNull JsonNode workflowFile) {
    return workflowFile.path("bam_file_size").asLong();
  }

  public static String getBamFileMd5sum(@NonNull JsonNode workflowFile) {
    return emptyToNull(workflowFile.path("bam_file_md5sum").asText());
  }

  public static String getSubmitterDonorId(@NonNull ObjectNode donor) {
    return donor.get("submitter_donor_id").textValue();
  }

  public static String getSubmitterSpecimenId(@NonNull JsonNode workflow) {
    return workflow.get("submitter_specimen_id").textValue();
  }

  public static String getSpecimenType(@NonNull JsonNode workflow) {
    return workflow.get("specimen_type").textValue();
  }

  public static String getSubmitterSampleId(@NonNull JsonNode workflow) {
    return workflow.get("submitter_sample_id").textValue();
  }

  public static String getGnosId(@NonNull JsonNode workflow) {
    return workflow.get("gnos_id").textValue();
  }

  public static ArrayNode getGnosRepo(@NonNull JsonNode workflow) {
    return (ArrayNode) workflow.get("gnos_repo");
  }

  public static String getGnosLastModified(@NonNull JsonNode workflow) {
    return workflow.get("gnos_last_modified").textValue();
  }

  public static JsonNode getFiles(@NonNull JsonNode workflow) {
    return workflow.path("files");
  }
}
