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
package org.icgc.dcc.etl.db.importer.repo.core;

import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.icgc.dcc.common.core.tcga.TCGAClient;
import org.icgc.dcc.etl.core.id.IdentifierClient;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.mongodb.MongoClientURI;

/**
 */
@RequiredArgsConstructor
public class RepositoryFileContext {

  /**
   * Configuration.
   */
  @Getter
  protected final MongoClientURI mongoUri;

  /**
   * Metadata.
   */
  @NonNull
  protected final Map<String, String> primarySites;

  /**
   * Caches.
   */
  private final Table<String, String, String> donorIdCache = HashBasedTable.create();
  private final Table<String, String, String> specimenIdCache = HashBasedTable.create();
  private final Table<String, String, String> sampleIdCache = HashBasedTable.create();

  /**
   * Dependencies.
   */
  @NonNull
  protected final IdentifierClient identifierClient;
  @NonNull
  protected final TCGAClient tcgaClient;

  @NonNull
  public String getPrimarySite(String projectCode) {
    return primarySites.get(projectCode);
  }

  @NonNull
  public String getDonorId(String submittedDonorId, String submittedProjectId) {
    String donorId = donorIdCache.get(submittedDonorId, submittedProjectId);
    if (donorId == null) {
      donorId = identifierClient.getDonorId(submittedDonorId, submittedProjectId);

      donorIdCache.put(submittedDonorId, submittedProjectId, donorId);
    }

    return filter(donorId);
  }

  @NonNull
  public String getSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    String specimenId = specimenIdCache.get(submittedSpecimenId, submittedProjectId);
    if (specimenId == null) {
      specimenId = identifierClient.getSpecimenId(submittedSpecimenId, submittedProjectId);

      specimenIdCache.put(submittedSpecimenId, submittedProjectId, specimenId);
    }

    return filter(specimenId);
  }

  @NonNull
  public String getSampleId(String submittedSampleId, String submittedProjectId) {
    String sampleId = sampleIdCache.get(submittedSampleId, submittedProjectId);
    if (sampleId == null) {
      sampleId = identifierClient.getSampleId(submittedSampleId, submittedProjectId);

      sampleIdCache.put(submittedSampleId, submittedProjectId, sampleId);
    }

    return filter(sampleId);
  }

  @NonNull
  protected Map<String, String> getTCGAUUIDs(Set<String> tcgaBarcodes) {
    return tcgaClient.getUUIDs(tcgaBarcodes);
  }

  @NonNull
  protected Map<String, String> getTCGABarcodes(Set<String> tcgaUuids) {
    return tcgaClient.getBarcodes(tcgaUuids);
  }

  //
  // TODO: Remove after identifier return 404s instead of "<id-prefix>null"
  //
  private static String filter(String id) {
    if (id.endsWith("null")) {
      return null;
    }

    return id;
  }

}