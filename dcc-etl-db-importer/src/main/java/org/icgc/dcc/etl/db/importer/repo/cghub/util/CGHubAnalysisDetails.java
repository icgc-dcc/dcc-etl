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
package org.icgc.dcc.etl.db.importer.repo.cghub.util;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@NoArgsConstructor(access = PRIVATE)
public final class CGHubAnalysisDetails {

  public static String getAnalysisId(JsonNode result) {
    return result.get("analysis_id").textValue();
  }

  public static long getFileSize(ObjectNode file) {
    return file.get("filesize").longValue();
  }

  public static String getFileName(ObjectNode file) {
    return file.get("filename").textValue();
  }

  public static String getParticipantId(JsonNode result) {
    return result.get("participant_id").textValue();
  }

  public static String getSampleId(JsonNode result) {
    return result.get("sample_id").textValue();
  }

  public static String getAliquotId(JsonNode result) {
    return result.get("aliquot_id").textValue();
  }

  public static String getLegacySampleId(JsonNode result) {
    return result.get("legacy_sample_id").textValue();
  }

  public static String getLegacySpecimenId(String legacySampleId) {
    return legacySampleId.substring(0, 20);
  }

  public static String getLegacyDonorId(String legacySampleId) {
    return legacySampleId.substring(0, 12);
  }

  public static String resolveProjectCode(String diseaseCode) {
    return diseaseCode + "-US";
  }

  public static String getFileName(JsonNode file) {
    return file.get("filename").textValue();
  }

  public static JsonNode getResults(JsonNode details) {
    return details.path("result_set").path("results");
  }

  public static JsonNode getFiles(JsonNode result) {
    return result.get("files");
  }

  public static String getLastModified(JsonNode result) {
    return result.get("last_modified").textValue();
  }

  public static boolean isBaiFile(JsonNode file) {
    val fileName = getFileName(file);

    return fileName.endsWith(".bam.bai");
  }

}
