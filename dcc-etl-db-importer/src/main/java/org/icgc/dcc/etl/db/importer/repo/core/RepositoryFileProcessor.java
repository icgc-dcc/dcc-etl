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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class RepositoryFileProcessor {

  /**
   * Dependencies.
   */
  @NonNull
  private final RepositoryFileContext context;

  @NonNull
  protected String resolvePrimarySite(String projectCode) {
    return context.getPrimarySite(projectCode);
  }

  @NonNull
  protected String resolveDonorId(String projectCode, String submittedDonorId) {
    return context.getDonorId(submittedDonorId, projectCode);
  }

  @NonNull
  protected String resolveSpecimenId(String projectCode, String submittedSpecimenId) {
    return context.getSpecimenId(submittedSpecimenId, projectCode);
  }

  @NonNull
  protected String resolveSampleId(String projectCode, String submittedSampleId) {
    return context.getSampleId(submittedSampleId, projectCode);
  }

  @NonNull
  protected Map<String, String> resolveTCGAUUIDs(Set<String> tcgaBarcodes) {
    return context.getTCGAUUIDs(tcgaBarcodes);
  }

  @NonNull
  protected Map<String, String> resolveTCGABarcodes(Set<String> tcgaUuids) {
    return context.getTCGABarcodes(tcgaUuids);
  }

  @NonNull
  public static final String formatDateTime(LocalDateTime dateTime) {
    return dateTime.format(DateTimeFormatter.ISO_INSTANT);
  }

  @NonNull
  public static final String formatDateTime(ZonedDateTime dateTime) {
    return dateTime.format(DateTimeFormatter.ISO_INSTANT);
  }

  @NonNull
  public static final String formatDateTime(Instant dateTime) {
    return dateTime.toString();
  }

}
