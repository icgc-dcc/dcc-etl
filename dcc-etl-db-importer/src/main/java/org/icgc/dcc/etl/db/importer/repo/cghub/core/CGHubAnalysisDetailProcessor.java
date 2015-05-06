/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.db.importer.repo.cghub.core;

import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubMetadata.CGHUB_BASE_URL;

import java.time.Instant;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;
import org.icgc.dcc.etl.db.importer.repo.util.FileRepositories;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@RequiredArgsConstructor
public class CGHubAnalysisDetailProcessor {

  @NonNull
  private final Map<String, String> primarySites;

  public Iterable<RepositoryFile> process(@NonNull String diseaseCode, @NonNull JsonNode details) {
    val cghubFiles = ImmutableList.<RepositoryFile> builder();

    val results = getResults(details);
    for (val result : results) {
      val files = getFiles(result);
      for (val file : files) {
        if (isBaiFile(file)) {
          continue;
        }

        val cghubFile = createCGHubFile(diseaseCode, result, (ObjectNode) file);

        cghubFiles.add(cghubFile);
      }
    }

    return cghubFiles.build();
  }

  private RepositoryFile createCGHubFile(String diseaseCode, JsonNode result, ObjectNode file) {
    val projectCode = resolveProjectName(diseaseCode);
    val legacySampleId = getLegacySampleId(result);
    val legacySpecimenId = getLegacySpecimenId(legacySampleId);
    val legacyDonorId = getLegacyDonorId(legacySampleId);

    val cghubFile = new RepositoryFile();
    cghubFile.setStudy(null);
    cghubFile.setAccess("controlled");

    cghubFile.setDataType(null);
    cghubFile.setDataSubType(null);
    cghubFile.setDataFormat(null);

    cghubFile.getRepository().setRepoType("GNOS");
    cghubFile.getRepository().setRepoOrg("CGHub");
    cghubFile.getRepository().setRepoEntityId(result.get("analysis_id").textValue());

    cghubFile.getRepository().getRepoServer().get(0).setRepoName("CGHub");
    cghubFile.getRepository().getRepoServer().get(0).setRepoCountry("USA");
    cghubFile.getRepository().getRepoServer().get(0).setRepoBaseUrl(CGHUB_BASE_URL);

    cghubFile.getRepository().setRepoPath(null);
    cghubFile.getRepository().setFileName(null);
    cghubFile.getRepository().setFileMd5sum(file.get("filename").textValue());
    cghubFile.getRepository().setFileSize(file.get("filesize").longValue());
    cghubFile.getRepository().setLastModified(
        FileRepositories.formatDateTime(Instant.parse(result.get("last_modified").textValue())));

    cghubFile.getDonor().setProjectCode(projectCode);
    cghubFile.getDonor().setPrimarySite(primarySites.get(projectCode));
    cghubFile.getDonor().setProgram(null);

    cghubFile.getDonor().setDonorId(null);
    cghubFile.getDonor().setSpecimenId(null);
    cghubFile.getDonor().setSampleId(null);

    cghubFile.getDonor().setSubmittedDonorId(null);
    cghubFile.getDonor().setSubmittedSpecimenId(getSampleId(result));
    cghubFile.getDonor().setSubmittedSampleId(getAliquotId(result));

    cghubFile.getDonor().setTcgaParticipantBarcode(legacyDonorId);
    cghubFile.getDonor().setTcgaSampleBarcode(legacySpecimenId);
    cghubFile.getDonor().setTcgaAliquotBarcode(legacySampleId);

    return cghubFile;
  }

  private static boolean isBaiFile(JsonNode file) {
    val fileName = getFileName(file);

    return fileName.endsWith(".bam.bai");
  }

  private static String getSampleId(JsonNode result) {
    return result.get("sample_id").textValue();
  }

  private static String getLegacySampleId(JsonNode result) {
    return result.get("legacy_sample_id").textValue();
  }

  private static String getLegacySpecimenId(String legacySampleId) {
    return legacySampleId.substring(0, 20);
  }

  private static String getLegacyDonorId(String legacySampleId) {
    return legacySampleId.substring(0, 12);
  }

  private static String resolveProjectName(String diseaseCode) {
    return diseaseCode + "-US";
  }

  private static String getAliquotId(JsonNode result) {
    return result.get("aliquot_id").textValue();
  }

  private static String getFileName(JsonNode file) {
    return file.get("filename").textValue();
  }

  private static JsonNode getResults(JsonNode details) {
    return details.path("result_set").path("results");
  }

  private static JsonNode getFiles(JsonNode result) {
    return result.get("files");
  }

}
