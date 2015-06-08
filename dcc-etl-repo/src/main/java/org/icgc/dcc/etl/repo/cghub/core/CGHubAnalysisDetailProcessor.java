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
package org.icgc.dcc.etl.repo.cghub.core;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getAliquotId;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getAnalysisId;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getAnalyteCode;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getChecksum;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getDiseaseAbbr;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getFileName;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getFileSize;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getFiles;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getLastModified;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getLegacyDonorId;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getLegacySampleId;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getLegacySpecimenId;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getLibraryStrategy;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getParticipantId;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getResults;
import static org.icgc.dcc.etl.repo.cghub.util.CGHubAnalysisDetails.getSampleId;
import static org.icgc.dcc.etl.repo.model.RepositoryProjects.getDiseaseCodeProject;
import static org.icgc.dcc.etl.repo.model.RepositoryServers.getCGHubServer;

import java.time.Instant;
import java.util.List;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.repo.core.RepositoryFileProcessor;
import org.icgc.dcc.etl.repo.model.RepositoryFile;
import org.icgc.dcc.etl.repo.model.RepositoryProjects.RepositoryProject;
import org.icgc.dcc.etl.repo.model.RepositoryServers.RepositoryServer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class CGHubAnalysisDetailProcessor extends RepositoryFileProcessor {

  /**
   * Constants.
   */
  private static final List<String> DNA_SEQ_ANALYTE_CODES = ImmutableList.of("D", "G", "W", "X");
  private static final List<String> RNA_SEQ_ANALYTE_CODES = ImmutableList.of("R", "T", "H");

  /**
   * Metadata.
   */
  @NonNull
  private final RepositoryServer cghubServer = getCGHubServer();

  public CGHubAnalysisDetailProcessor(RepositoryFileContext context) {
    super(context);
  }

  @NonNull
  public Iterable<RepositoryFile> processDetails(Iterable<ObjectNode> details) {
    val analysisFiles = stream(details)
        .flatMap(detail -> stream(getResults(detail)))
        .flatMap(result -> stream(processResult(result)))
        .filter(analysisFile -> analysisFile.getDonor().hasDonorId()) // Filter out non-ICGC donors
        .collect(toImmutableList());

    assignStudy(analysisFiles);

    return analysisFiles;
  }

  private Iterable<RepositoryFile> processResult(JsonNode result) {
    return stream(getFiles(result))
        .filter(file -> isBamFile(file))
        .map(file -> createAnalysisFile(result, file))
        .collect(toImmutableList());
  }

  private RepositoryFile createAnalysisFile(JsonNode result, JsonNode file) {
    val project = resolveProject(result);
    val projectCode = project.getProjectCode();

    val legacySampleId = getLegacySampleId(result);
    val legacySpecimenId = getLegacySpecimenId(legacySampleId);
    val legacyDonorId = getLegacyDonorId(legacySampleId);

    val analysisId = getAnalysisId(result);
    val fileName = getFileName(file);

    val analysisFile = new RepositoryFile()
        .setId(resolveFileId(analysisId, fileName))
        .setStudy(null) // N/A
        .setAccess("controlled");

    analysisFile.getDataType()
        .setDataType(resolveDataType(result))
        .setDataFormat("BAM")
        .setExperimentalStrategy(getLibraryStrategy(result));

    analysisFile.getRepository()
        .setRepoType(cghubServer.getType().getId())
        .setRepoOrg(cghubServer.getSource().getId())
        .setRepoEntityId(analysisId);

    analysisFile.getRepository().getRepoServer().get(0)
        .setRepoName(cghubServer.getName())
        .setRepoCode(cghubServer.getCode())
        .setRepoCountry(cghubServer.getCountry())
        .setRepoBaseUrl(cghubServer.getBaseUrl());

    analysisFile.getRepository()
        .setRepoMetadataPath(cghubServer.getType().getMetadataPath())
        .setRepoDataPath(cghubServer.getType().getDataPath())
        .setFileName(fileName)
        .setFileMd5sum(getChecksum(file))
        .setFileSize(getFileSize(file))
        .setLastModified(resolveLastModified(result));

    analysisFile.getDonor()
        .setPrimarySite(context.getPrimarySite(projectCode))
        .setProjectCode(projectCode)
        .setProgram(project.getProgram())
        .setStudy(null) // Set downstream

        .setDonorId(context.getDonorId(legacyDonorId, projectCode))
        .setSpecimenId(context.getSpecimenId(legacySpecimenId, projectCode))
        .setSampleId(context.getSampleId(legacySampleId, projectCode))

        .setSubmittedDonorId(getParticipantId(result))
        .setSubmittedSpecimenId(getSampleId(result))
        .setSubmittedSampleId(getAliquotId(result))

        .setTcgaParticipantBarcode(legacyDonorId)
        .setTcgaSampleBarcode(legacySpecimenId)
        .setTcgaAliquotBarcode(legacySampleId);

    return analysisFile;
  }

  private void assignStudy(Iterable<RepositoryFile> analysisFiles) {
    for (val analysisFile : analysisFiles) {
      val donor = analysisFile.getDonor();
      val pcawg = context.isPCAWGSubmittedDonorId(donor.getProjectCode(), donor.getSubmittedDonorId());
      if (pcawg) {
        donor.setStudy(PCAWG_STUDY_VALUE);
      }
    }
  }

  private static RepositoryProject resolveProject(JsonNode result) {
    val diseaseCode = getDiseaseAbbr(result);

    return getDiseaseCodeProject(diseaseCode).orNull();
  }

  private static String resolveDataType(JsonNode result) {
    val analyteCode = getAnalyteCode(result);
    if (DNA_SEQ_ANALYTE_CODES.contains(analyteCode)) {
      return "DNA-Seq";
    } else if (RNA_SEQ_ANALYTE_CODES.contains(analyteCode)) {
      return "RNA-Seq";
    }

    return null;
  }

  private static String resolveLastModified(JsonNode result) {
    val instant = Instant.parse(getLastModified(result));

    return instant.toString();
  }

  private static boolean isBamFile(JsonNode file) {
    return getFileName(file).endsWith(".bam");
  }

}
