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
package org.icgc.dcc.etl.loader.core;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import org.icgc.dcc.common.core.model.ClinicalType;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FileTypes.FileSubType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes the non-clinical data provided for a submission (since clinical data is always provided).
 */
@NoArgsConstructor
@Accessors(chain = true)
@ToString
public final class ProvidedDataSubmissionDigest {

  @Setter
  @JsonProperty
  private Map<FeatureType, Set<FileSubType>> featureDataTypes;

  @Setter
  @JsonProperty
  private Map<ClinicalType, Set<FileSubType>> supplementalDataTypes;

  @JsonIgnore
  public Set<FeatureType> getFeatureTypes() {
    return featureDataTypes.keySet();
  }

  @JsonIgnore
  public Set<ClinicalType> getSupplementalTypes() {
    return supplementalDataTypes.keySet();
  }

  @JsonIgnore
  public Set<FileSubType> getAllSupplementalSubTypes() {
    return supplementalDataTypes.values()
        .stream()
        .flatMap(l -> l.stream()).
        collect(Collectors.toSet());
  }

  @JsonIgnore
  public Set<FileSubType> getSubTypes(FeatureType type) {
    return featureDataTypes.get(type);
  }

  @JsonIgnore
  public Set<FileSubType> getSubTypes(ClinicalType type) {
    return supplementalDataTypes.get(type);
  }

}
