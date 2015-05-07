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
package org.icgc.dcc.etl.core.id;

import static com.google.common.base.Joiner.on;
import static com.google.common.hash.Hashing.md5;

import java.io.IOException;

import lombok.NoArgsConstructor;

import com.google.common.base.Joiner;
import com.google.common.hash.HashFunction;

/**
 * Stateless hash based {@link IdentifierClient} implementation that returns a stable id based on it it's inputs.
 */
@NoArgsConstructor
public class HashIdentifierClient implements IdentifierClient {

  /**
   * Id generation strategy state.
   */
  private static final HashFunction MD5 = md5();
  private static final Joiner JOINER = on(":");

  /**
   * Required for reflection in Loader
   */
  public HashIdentifierClient(String serviceUri, String release) {
    // Empty
  }

  @Override
  public String getDonorId(String submittedDonorId, String submittedProjectId) {
    return "DO" + generateId(
        submittedDonorId,
        submittedProjectId);
  }

  @Override
  public String getSampleId(String submittedSampleId, String submittedProjectId) {
    return "SA" + generateId(
        submittedSampleId,
        submittedProjectId);
  }

  @Override
  public String getSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    return "SP" + generateId(
        submittedSpecimenId,
        submittedProjectId);
  }

  @Override
  public String getMutationId(String chromosome, String chromosomeStart, String chromosomeEnd, String mutation,
      String mutationType, String assemblyVersion) {
    return "MU" + generateId(
        chromosome,
        chromosomeStart,
        chromosomeEnd,
        mutation,
        mutationType,
        assemblyVersion);
  }

  @Override
  public String createDonorId(String submittedDonorId, String submittedProjectId) {
    return getDonorId(submittedDonorId, submittedProjectId);
  }

  @Override
  public String createMutationId(String chromosome, String chromosomeStart, String chromosomeEnd, String mutation,
      String mutationType, String assemblyVersion) {
    return getMutationId(chromosome, chromosomeStart, chromosomeEnd, mutation, mutationType, assemblyVersion);
  }

  @Override
  public String createSampleId(String submittedSampleId, String submittedProjectId) {
    return getSampleId(submittedSampleId, submittedProjectId);
  }

  @Override
  public String createSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    return getSpecimenId(submittedSpecimenId, submittedProjectId);
  }

  @Override
  public void close() throws IOException {
  }

  private static String generateId(String... keys) {
    return MD5.hashUnencodedChars(JOINER.join(keys)).toString();
  }

}
