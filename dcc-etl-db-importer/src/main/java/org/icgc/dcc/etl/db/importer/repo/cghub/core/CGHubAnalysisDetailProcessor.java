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

import static org.icgc.dcc.etl.db.importer.repo.model.FileRepositoryOrg.CGHUB;

import java.time.Instant;
import java.util.Map;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.core.id.IdentifierClient;
import org.icgc.dcc.etl.db.importer.repo.core.RepositoryTypeProcessor;
import org.icgc.dcc.etl.db.importer.repo.model.FileRepositories;
import org.icgc.dcc.etl.db.importer.repo.model.FileRepositories.RepositoryServer;
import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class CGHubAnalysisDetailProcessor extends RepositoryTypeProcessor {

  /**
   * Metadata.
   */
  @NonNull
  private final RepositoryServer server;

  public CGHubAnalysisDetailProcessor(Map<String, String> primarySites, IdentifierClient identifierClient) {
    super(primarySites, identifierClient);
    this.server = resolveServer();
  }

  public Iterable<RepositoryFile> process(@NonNull String diseaseCode, @NonNull JsonNode details) {
    val analysisFiles = ImmutableList.<RepositoryFile> builder();

    val results = getResults(details);
    for (val result : results) {
      val files = getFiles(result);
      for (val file : files) {
        if (isBaiFile(file)) {
          continue;
        }

        val analysisFile = createAnalysisFile(diseaseCode, result, (ObjectNode) file);

        analysisFiles.add(analysisFile);
      }
    }

    return analysisFiles.build();
  }

  private RepositoryFile createAnalysisFile(String diseaseCode, JsonNode result, ObjectNode file) {
    val projectCode = resolveProjectCode(diseaseCode);
    val legacySampleId = getLegacySampleId(result);
    val legacySpecimenId = getLegacySpecimenId(legacySampleId);
    val legacyDonorId = getLegacyDonorId(legacySampleId);

    val analysisFile = new RepositoryFile();
    analysisFile.setStudy(null);
    analysisFile.setAccess("controlled");

    analysisFile.setDataType(null);
    analysisFile.setDataSubType(null);
    analysisFile.setDataFormat(null);

    analysisFile.getRepository().setRepoType(server.getType().getId());
    analysisFile.getRepository().setRepoOrg(server.getOrg().getId());
    analysisFile.getRepository().setRepoEntityId(getAnalysisId(result));

    analysisFile.getRepository().getRepoServer().get(0).setRepoName(server.getName());
    analysisFile.getRepository().getRepoServer().get(0).setRepoCountry(server.getCountry());
    analysisFile.getRepository().getRepoServer().get(0).setRepoBaseUrl(server.getBaseUrl());

    analysisFile.getRepository().setRepoPath(null);
    analysisFile.getRepository().setFileName(getFileName(file));
    analysisFile.getRepository().setFileMd5sum(null);
    analysisFile.getRepository().setFileSize(getFileSize(file));
    analysisFile.getRepository().setLastModified(resolveLastModified(result));

    analysisFile.getDonor().setProjectCode(projectCode);
    analysisFile.getDonor().setPrimarySite(resolvePrimarySite(projectCode));
    analysisFile.getDonor().setProgram(null);

    analysisFile.getDonor().setDonorId(resolveDonorId(projectCode, legacyDonorId));
    analysisFile.getDonor().setSpecimenId(resolveSpecimenId(projectCode, legacySpecimenId));
    analysisFile.getDonor().setSampleId(resolveSampleId(projectCode, legacySampleId));

    analysisFile.getDonor().setSubmittedDonorId(null);
    analysisFile.getDonor().setSubmittedSpecimenId(getSampleId(result));
    analysisFile.getDonor().setSubmittedSampleId(getAliquotId(result));

    analysisFile.getDonor().setTcgaParticipantBarcode(legacyDonorId);
    analysisFile.getDonor().setTcgaSampleBarcode(legacySpecimenId);
    analysisFile.getDonor().setTcgaAliquotBarcode(legacySampleId);

    return analysisFile;
  }

  private static RepositoryServer resolveServer() {
    for (val server : FileRepositories.getServers()) {
      if (server.getOrg() == CGHUB) {
        return server;
      }
    }

    return null;
  }

  private static String resolveLastModified(JsonNode result) {
    return FileRepositories.formatDateTime(Instant.parse(result.get("last_modified").textValue()));
  }

  private static long getFileSize(ObjectNode file) {
    return file.get("filesize").longValue();
  }

  private static String getFileName(ObjectNode file) {
    return file.get("filename").textValue();
  }

  private static String getAnalysisId(JsonNode result) {
    return result.get("analysis_id").textValue();
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

  private static String resolveProjectCode(String diseaseCode) {
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

  private static boolean isBaiFile(JsonNode file) {
    val fileName = getFileName(file);
  
    return fileName.endsWith(".bam.bai");
  }

}
