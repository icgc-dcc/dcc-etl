/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.icgc.dcc.etl.loader.core;

import static com.google.common.collect.ImmutableSet.copyOf;

import java.util.Map;
import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.val;
import lombok.experimental.Accessors;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

/**
 * Describes the data provided for a release.
 */
@NoArgsConstructor
@Accessors(chain = true)
@ToString
public class ProvidedDataReleaseDigest {

  /**
   * Project key to project-specific data digest map.
   */
  @Setter
  @JsonProperty
  private Map<String, ProvidedDataSubmissionDigest> dataSubmissionDigests;

  @JsonIgnore
  public Set<String> getProjectKeys() {
    return copyOf(dataSubmissionDigests.keySet());
  }

  @JsonIgnore
  public ProvidedDataSubmissionDigest getDataSubmissionDigest(String projectKey) {
    return dataSubmissionDigests.get(projectKey);
  }

  /**
   * Returns the set of {@link FeatureType} provided in the release (in the various submissions).
   */
  @JsonIgnore
  public Set<FeatureType> getFeatureTypes() {
    val builder = new ImmutableSet.Builder<FeatureType>();
    for (String projectKey : getProjectKeys()) {
      builder.addAll(
          getDataSubmissionDigest(projectKey)
              .getFeatureTypes());
    }
    return builder.build();
  }

}
