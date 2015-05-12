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
package org.icgc.dcc.etl.repo.cghub.util;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@NoArgsConstructor(access = PRIVATE)
public final class CGHubAnalysisDetails {

  public static JsonNode getResults(@NonNull JsonNode details) {
    return details.path("result_set").path("results");
  }

  public static JsonNode getFiles(@NonNull JsonNode result) {
    return result.get("files");
  }

  public static String getAnalyteCode(@NonNull JsonNode result) {
    return result.get("analyte_code").textValue();
  }

  public static String getDiseaseAbbr(@NonNull JsonNode result) {
    return result.get("disease_abbr").textValue();
  }

  public static String getLibraryStrategy(@NonNull JsonNode result) {
    return result.get("library_strategy").textValue();
  }

  public static String getAnalysisId(@NonNull JsonNode result) {
    return result.get("analysis_id").textValue();
  }

  public static long getFileSize(@NonNull ObjectNode file) {
    return file.get("filesize").longValue();
  }

  public static String getFileName(@NonNull ObjectNode file) {
    return file.get("filename").textValue();
  }

  public static String getParticipantId(@NonNull JsonNode result) {
    return result.get("participant_id").textValue();
  }

  public static String getSampleId(@NonNull JsonNode result) {
    return result.get("sample_id").textValue();
  }

  public static String getAliquotId(@NonNull JsonNode result) {
    return result.get("aliquot_id").textValue();
  }

  public static String getLegacySampleId(@NonNull JsonNode result) {
    return result.get("legacy_sample_id").textValue();
  }

  public static String getLegacySpecimenId(@NonNull String legacySampleId) {
    return legacySampleId.substring(0, 16);
  }

  public static String getLegacyDonorId(@NonNull String legacySampleId) {
    return legacySampleId.substring(0, 12);
  }

  public static String getFileName(@NonNull JsonNode file) {
    return file.get("filename").textValue();
  }

  public static String getLastModified(@NonNull JsonNode result) {
    return result.get("last_modified").textValue();
  }

}
