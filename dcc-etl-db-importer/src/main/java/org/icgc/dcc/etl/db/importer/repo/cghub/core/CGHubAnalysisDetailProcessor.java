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

import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getAliquotId;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getAnalysisId;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getDiseaseAbbr;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getFileName;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getFileSize;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getFiles;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getLastModified;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getLegacyDonorId;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getLegacySampleId;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getLegacySpecimenId;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getParticipantId;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getResults;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubAnalysisDetails.getSampleId;
import static org.icgc.dcc.etl.db.importer.repo.model.RepositoryProjects.getDiseaseCodeProject;
import static org.icgc.dcc.etl.db.importer.repo.model.RepositoryServers.getCGHubServer;

import java.time.Instant;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.db.importer.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.db.importer.repo.core.RepositoryFileProcessor;
import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;
import org.icgc.dcc.etl.db.importer.repo.model.RepositoryServers.RepositoryServer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class CGHubAnalysisDetailProcessor extends RepositoryFileProcessor {

  /**
   * Metadata.
   */
  @NonNull
  private final RepositoryServer server = getCGHubServer();

  public CGHubAnalysisDetailProcessor(RepositoryFileContext context) {
    super(context);
  }

  @NonNull
  public Iterable<RepositoryFile> processDetails(Iterable<ObjectNode> details) {
    val analysisFiles = ImmutableList.<RepositoryFile> builder();

    for (val detail : details) {
      val results = getResults(detail);
      for (val result : results) {
        val files = getFiles(result);
        for (val file : files) {
          if (isBaiFile(file)) {
            continue;
          }

          val analysisFile = createAnalysisFile(result, (ObjectNode) file);
          analysisFiles.add(analysisFile);
        }
      }
    }

    return analysisFiles.build();
  }

  private RepositoryFile createAnalysisFile(JsonNode result, ObjectNode file) {
    val diseaseCode = getDiseaseAbbr(result);
    val project = getDiseaseCodeProject(diseaseCode);
    val projectCode = project.getProjectCode();
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

    analysisFile.getRepository().setRepoPath(server.getType().getPath());
    analysisFile.getRepository().setFileName(getFileName(file));
    analysisFile.getRepository().setFileMd5sum(null);
    analysisFile.getRepository().setFileSize(getFileSize(file));
    analysisFile.getRepository().setLastModified(resolveLastModified(result));

    analysisFile.getDonor().setProjectCode(projectCode);
    analysisFile.getDonor().setPrimarySite(resolvePrimarySite(projectCode));
    analysisFile.getDonor().setProgram(project.getProgram());

    analysisFile.getDonor().setDonorId(resolveDonorId(projectCode, legacyDonorId));
    analysisFile.getDonor().setSpecimenId(resolveSpecimenId(projectCode, legacySpecimenId));
    analysisFile.getDonor().setSampleId(resolveSampleId(projectCode, legacySampleId));

    analysisFile.getDonor().setSubmittedDonorId(getParticipantId(result));
    analysisFile.getDonor().setSubmittedSpecimenId(getSampleId(result));
    analysisFile.getDonor().setSubmittedSampleId(getAliquotId(result));

    analysisFile.getDonor().setTcgaParticipantBarcode(legacyDonorId);
    analysisFile.getDonor().setTcgaSampleBarcode(legacySpecimenId);
    analysisFile.getDonor().setTcgaAliquotBarcode(legacySampleId);

    return analysisFile;
  }

  private static String resolveLastModified(JsonNode result) {
    val text = getLastModified(result);

    return formatDateTime(Instant.parse(text));
  }

  private static boolean isBaiFile(JsonNode file) {
    val fileName = getFileName(file);

    return fileName.endsWith(".bam.bai");
  }

}
