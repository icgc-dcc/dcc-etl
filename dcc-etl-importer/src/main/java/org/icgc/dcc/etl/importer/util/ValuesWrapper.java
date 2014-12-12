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
package org.icgc.dcc.etl.importer.util;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.limit;

import java.util.Set;

import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Builder;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;

/**
 * Represents a wrapper for values that are explicitly specified to be included or excluded in the import.
 */
@Value
@Builder
public class ValuesWrapper {

  public static final Optional<ValuesWrapper> ABSENT_VALUES_WRAPPER = Optional.absent();

  /**
   * Surrogate key that identifies the documents.
   */
  @NonNull
  private final String surrogateKey;

  /**
   * Values to act upon.
   */
  @NonNull
  private final Set<String> values;

  /**
   * Determines if a document has a match in the values specified and for the given surrogate key.
   */
  public boolean isMatch(JsonNode sourceDocument) {
    JsonNode value = sourceDocument.path(surrogateKey);
    checkState(!value.isMissingNode() && value.isTextual(),
        "Expecting a field '{}' in document '{}'", surrogateKey, sourceDocument);
    return values.contains(value.asText());
  }

  @Override
  public String toString() {
    int displayThreshold = 100;
    return toStringHelper(ValuesWrapper.class)
        .add("surrogateKey", surrogateKey)
        .add(
            "values",
            values.size() < displayThreshold ?
                values :
                limit(values, displayThreshold) + " [...]")
        .toString();
  }
}
