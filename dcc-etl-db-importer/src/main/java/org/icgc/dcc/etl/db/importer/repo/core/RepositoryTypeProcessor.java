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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.icgc.dcc.etl.core.id.IdentifierClient;

@RequiredArgsConstructor
public abstract class RepositoryTypeProcessor {

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
  protected String resolvePrimarySite(String projectCode) {
    return primarySites.get(projectCode);
  }

  @NonNull
  protected String resolveDonorId(String projectCode, String submittedDonorId) {
    return identifierClient.getDonorId(submittedDonorId, projectCode);
  }

  @NonNull
  protected String resolveSpecimenId(String projectCode, String submittedSpecimenId) {
    return identifierClient.getSpecimenId(submittedSpecimenId, projectCode);
  }

  @NonNull
  protected String resolveSampleId(String projectCode, String submittedSampleId) {
    return identifierClient.getSampleId(submittedSampleId, projectCode);
  }

}
