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
import static org.icgc.dcc.etl.repo.util.Collectors.toImmutableList;
import static org.icgc.dcc.etl.repo.util.Streams.stream;

import java.time.Instant;
import java.util.List;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.repo.core.RepositoryFileProcessor;
import org.icgc.dcc.etl.repo.model.RepositoryFile;
import org.icgc.dcc.etl.repo.model.RepositoryFile.RepositoryFileDataType;
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
    return stream(details)
        .flatMap(detail -> stream(getResults(detail)))
        .flatMap(result -> stream(processResult(result)))
        .collect(toImmutableList());
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

    final @val java.lang.String dataType = resolveDataType(result);
    val experimentalStrategy = getLibraryStrategy(result);

    val analysisFile = new RepositoryFile();
    analysisFile
        .setStudy(null)
        .setAccess("controlled");

    analysisFile
        .getDataTypes().add(
            new RepositoryFileDataType()
                .setDataType(dataType)
                .setDataFormat("BAM")
                .setExperimentalStrategy(experimentalStrategy));

    analysisFile.getRepository()
        .setRepoType(cghubServer.getType().getId())
        .setRepoOrg(cghubServer.getSource().getId())
        .setRepoEntityId(getAnalysisId(result));

    analysisFile.getRepository().getRepoServer().get(0)
        .setRepoName(cghubServer.getName())
        .setRepoCode(cghubServer.getCode())
        .setRepoCountry(cghubServer.getCountry())
        .setRepoBaseUrl(cghubServer.getBaseUrl());

    analysisFile.getRepository()
        .setRepoMetadataPath(cghubServer.getType().getMetadataPath())
        .setRepoDataPath(cghubServer.getType().getDataPath())
        .setFileName(getFileName(file))
        .setFileMd5sum(getChecksum(file))
        .setFileSize(getFileSize(file))
        .setLastModified(resolveLastModified(result));

    analysisFile.getDonor()
        .setPrimarySite(resolvePrimarySite(projectCode))
        .setProjectCode(projectCode)
        .setProgram(project.getProgram())
        .setStudy(null)

        .setDonorId(resolveDonorId(projectCode, legacyDonorId))
        .setSpecimenId(resolveSpecimenId(projectCode, legacySpecimenId))
        .setSampleId(resolveSampleId(projectCode, legacySampleId))

        .setSubmittedDonorId(getParticipantId(result))
        .setSubmittedSpecimenId(getSampleId(result))
        .setSubmittedSampleId(getAliquotId(result))

        .setTcgaParticipantBarcode(legacyDonorId)
        .setTcgaSampleBarcode(legacySpecimenId)
        .setTcgaAliquotBarcode(legacySampleId);

    return analysisFile;
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

    return formatDateTime(instant);
  }

  private static boolean isBamFile(JsonNode file) {
    return getFileName(file).endsWith(".bam");
  }

}
