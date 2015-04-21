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
package org.icgc.dcc.etl.loader.flow;

import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.generatedFields;

import org.icgc.dcc.common.core.model.ClinicalType;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;

import cascading.tuple.Fields;

/**
 * Helper class for gathering summary information.
 */
public class SummaryHelper {

  /**
   * The donor ID used for summarization.
   */
  public static final String SUMMARY_DONOR_ID = DONOR_ID;

  /**
   * Temporary fields to avoid collision in joins.
   */
  public static final String SUMMARY_TEMP_DONOR_ID = "_summary_temp_" + SUMMARY_DONOR_ID;

  /**
   * See {@link SUMMARY_DONOR_ID}.
   */
  public static Fields getDonorIdField() {
    return generatedFields(SUMMARY_DONOR_ID);
  }

  /**
   * See {@link SUMMARY_TEMP_DONOR_ID}.
   */
  public static Fields getSummaryTempDonorIdField() {
    return generatedFields(SUMMARY_TEMP_DONOR_ID);
  }

  /**
   * Returns a {@link Fields} to hold summary information for a given {@link FeatureType}.
   */
  public static Fields getSummaryValueField(FeatureType featureType) {
    return new Fields(featureType.getSummaryFieldName());
  }

  /**
   * Returns a {@link Fields} to hold summary information on the {@link FeatureType}s that were provided in a given
   * submission.
   */
  public static Fields getAvailableDataTypeField() {
    return new Fields(AVAILABLE_DATA_TYPES);
  }

  /**
   * Returns a {@link Fields} to hold summary information for a given {@link ClinicalType}.
   */
  public static Fields getSummaryValueField(ClinicalType clinicalType) {
    return new Fields(clinicalType.getSummaryFieldName());
  }

}
