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
package org.icgc.dcc.etl.core.id;

import java.io.IOException;
import java.util.Optional;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class ForwardingIdentifierClient implements IdentifierClient {

  /**
   * Dependencies.
   */
  @NonNull
  protected final IdentifierClient delegate;

  @Override
  public Optional<String> getDonorId(String submittedDonorId, String submittedProjectId) {
    return delegate.getDonorId(submittedDonorId, submittedProjectId);
  }

  @Override
  public Optional<String> getSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    return delegate.getSpecimenId(submittedSpecimenId, submittedProjectId);
  }

  @Override
  public Optional<String> getSampleId(String submittedSampleId, String submittedProjectId) {
    return delegate.getSampleId(submittedSampleId, submittedProjectId);
  }

  @Override
  public Optional<String> getMutationId(String chromosome, String chromosomeStart, String chromosomeEnd,
      String mutation,
      String mutationType, String assemblyVersion) {
    return delegate.getMutationId(chromosome, chromosomeStart, chromosomeEnd, mutation, mutationType, assemblyVersion);
  }

  @Override
  public String createDonorId(String submittedDonorId, String submittedProjectId) {
    return delegate.createDonorId(submittedDonorId, submittedProjectId);
  }

  @Override
  public String createSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    return delegate.createSpecimenId(submittedSpecimenId, submittedProjectId);
  }

  @Override
  public String createSampleId(String submittedSampleId, String submittedProjectId) {
    return delegate.createSampleId(submittedSampleId, submittedProjectId);
  }

  @Override
  public String createMutationId(String chromosome, String chromosomeStart, String chromosomeEnd, String mutation,
      String mutationType, String assemblyVersion) {
    return delegate.createMutationId(chromosome, chromosomeStart, chromosomeEnd, mutation, mutationType,
        assemblyVersion);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

}
