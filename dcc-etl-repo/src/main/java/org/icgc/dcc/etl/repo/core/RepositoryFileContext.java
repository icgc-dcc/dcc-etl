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
package org.icgc.dcc.etl.repo.core;

import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.etl.repo.pcawg.core.PCAWGDonorIndentifier.qualifyDonorId;

import java.util.Map;
import java.util.Set;

import org.icgc.dcc.common.core.tcga.TCGAClient;
import org.icgc.dcc.etl.core.id.IdentifierClient;
import org.icgc.dcc.etl.repo.aws.reader.AWSS3TransferJobReader;
import org.icgc.dcc.etl.repo.pcawg.core.PCAWGDonorIndentifier;

import com.mongodb.MongoClientURI;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
public class RepositoryFileContext {

  /**
   * Configuration.
   */
  @Getter
  @NonNull
  private final MongoClientURI mongoUri;
  @Getter
  @NonNull
  private final String esUri;
  private final boolean readOnly = false;

  /**
   * Metadata.
   */
  @NonNull
  private final Map<String, String> primarySites;

  /**
   * Dependencies.
   */
  @NonNull
  private final IdentifierClient identifierClient;
  @NonNull
  private final TCGAClient tcgaClient;

  /**
   * Data.
   */
  @Getter(lazy = true, value = PRIVATE)
  private final Set<String> pcawgSubmittedDonorIds = new PCAWGDonorIndentifier().identifyDonorIds();
  @Getter(lazy = true, value = PRIVATE)
  private final Set<String> awsS3ObjectIds = new AWSS3TransferJobReader().readObjectIds();

  @NonNull
  public String getPrimarySite(String projectCode) {
    return primarySites.get(projectCode);
  }

  @NonNull
  public boolean isPCAWGSubmittedDonorId(String projectCode, String submittedDonorId) {
    return getPcawgSubmittedDonorIds().contains(qualifyDonorId(projectCode, submittedDonorId));
  }

  public boolean isAWSS3ObjectId(String objectId) {
    return getAwsS3ObjectIds().contains(objectId);
  }

  @NonNull
  public String getDonorId(String submittedDonorId, String submittedProjectId) {
    return identifierClient.getDonorId(submittedDonorId, submittedProjectId).orElse(null);
  }

  @NonNull
  public String ensureDonorId(String submittedDonorId, String submittedProjectId) {
    if (readOnly) {
      return getDonorId(submittedDonorId, submittedProjectId);
    }

    return identifierClient.createDonorId(submittedDonorId, submittedProjectId);
  }

  @NonNull
  public String getSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    return identifierClient.getSpecimenId(submittedSpecimenId, submittedProjectId).orElse(null);
  }

  @NonNull
  public String ensureSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    if (readOnly) {
      return getSpecimenId(submittedSpecimenId, submittedProjectId);
    }

    return identifierClient.createSpecimenId(submittedSpecimenId, submittedProjectId);
  }

  @NonNull
  public String getSampleId(String submittedSampleId, String submittedProjectId) {
    return identifierClient.getSampleId(submittedSampleId, submittedProjectId).orElse(null);
  }

  @NonNull
  public String ensureSampleId(String submittedSampleId, String submittedProjectId) {
    if (readOnly) {
      return getSampleId(submittedSampleId, submittedProjectId);
    }

    return identifierClient.createSampleId(submittedSampleId, submittedProjectId);
  }

  @NonNull
  public Map<String, String> getTCGAUUIDs(Set<String> tcgaBarcodes) {
    return tcgaClient.getUUIDs(tcgaBarcodes);
  }

  @NonNull
  public Map<String, String> getTCGABarcodes(Set<String> tcgaUuids) {
    return tcgaClient.getBarcodes(tcgaUuids);
  }

}