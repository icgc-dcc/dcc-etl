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

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CachingIdentifierClient extends ForwardingIdentifierClient {

  /**
   * Caches.
   */
  @NonNull
  private final LoadingCache<Key, String> donorIdCache;
  private final LoadingCache<Key, String> specimenIdCache;
  private final LoadingCache<Key, String> sampleIdCache;

  public CachingIdentifierClient(IdentifierClient delegate) {
    super(delegate);

    this.donorIdCache =
        createCache(key -> key.isCreate() ?
            delegate.createDonorId(key.getSubmittedId(), key.getSubmittedProjectId()) :
            delegate.getDonorId(key.getSubmittedId(), key.getSubmittedProjectId()));
    this.specimenIdCache =
        createCache(key -> key.isCreate() ?
            delegate.createSpecimenId(key.getSubmittedId(), key.getSubmittedProjectId()) :
            delegate.getSpecimenId(key.getSubmittedId(), key.getSubmittedProjectId()));
    this.sampleIdCache =
        createCache(key -> key.isCreate() ?
            delegate.createSampleId(key.getSubmittedId(), key.getSubmittedProjectId()) :
            delegate.getSampleId(key.getSubmittedId(), key.getSubmittedProjectId()));
  }

  @Override
  @SneakyThrows
  public String getDonorId(String submittedDonorId, String submittedProjectId) {
    return donorIdCache.get(new Key(submittedDonorId, submittedProjectId, false));
  }

  @Override
  @SneakyThrows
  public String getSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    return specimenIdCache.get(new Key(submittedSpecimenId, submittedProjectId, false));
  }

  @Override
  @SneakyThrows
  public String getSampleId(String submittedSampleId, String submittedProjectId) {
    return sampleIdCache.get(new Key(submittedSampleId, submittedProjectId, false));
  }

  @Override
  @SneakyThrows
  public String createDonorId(String submittedDonorId, String submittedProjectId) {
    return donorIdCache.get(new Key(submittedDonorId, submittedProjectId, true));
  }

  @Override
  @SneakyThrows
  public String createSpecimenId(String submittedSpecimenId, String submittedProjectId) {
    return specimenIdCache.get(new Key(submittedSpecimenId, submittedProjectId, true));
  }

  @Override
  @SneakyThrows
  public String createSampleId(String submittedSampleId, String submittedProjectId) {
    return sampleIdCache.get(new Key(submittedSampleId, submittedProjectId, true));
  }

  private static LoadingCache<Key, String> createCache(Function<Key, String> loader) {
    return CacheBuilder.newBuilder().build(CacheLoader.from(loader));
  }

  @Value
  private static class Key {

    String submittedId;
    String submittedProjectId;
    boolean create;

  }

}
