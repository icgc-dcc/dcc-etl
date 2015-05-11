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
package org.icgc.dcc.etl.repo.cghub.util;

import static lombok.AccessLevel.PRIVATE;

import java.util.Map;

import lombok.NoArgsConstructor;

import com.google.common.collect.ImmutableMap;

@NoArgsConstructor(access = PRIVATE)
public final class CGHubConverters {

  /**
   * Constants.
   */
  public static final String CGHUB_BASE_URL = "https://cghub.ucsc.edu/";

  private static final Map<String, String> SAMPLE_TYPE_CODE_MAPPING = ImmutableMap.<String, String> builder()
      .put("01", "Primary solid Tumor")
      .put("02", "Recurrent Solid Tumor")
      .put("03", "Primary Blood Derived Cancer - Peripheral Blood")
      .put("04", "Recurrent Blood Derived Cancer - Bone Marrow")
      .put("05", "Additional - New Primary")
      .put("06", "Metastatic")
      .put("07", "Additional Metastatic")
      .put("08", "Human Tumor Original Cells")
      .put("09", "Primary Blood Derived Cancer - Bone Marrow")
      .put("10", "Blood Derived Normal")
      .put("11", "Solid Tissue Normal")
      .put("12", "Buccal Cell Normal")
      .put("13", "EBV Immortalized Normal")
      .put("14", "Bone Marrow Normal")
      .put("20", "Control Analyte")
      .put("40", "Recurrent Blood Derived Cancer - Peripheral Blood")
      .put("50", "Cell Lines")
      .put("60", "Primary Xenograft Tissue")
      .put("61", "Cell Line Derived Xenograft Tissue")
      .build();

  private static final Map<String, String> ANALYTE_CODE_MAPPING = ImmutableMap.<String, String> builder()
      .put("D", "DNA")
      .put("G", "Whole Genome Amplification (WGA) produced using GenomePlex (Rubicon) DNA")
      .put("H", "mirVana RNA (Allprep DNA) produced by hybrid protocol")
      .put("R", "RNA")
      .put("T", "Total RNA")
      .put("W", "Whole Genome Amplification (WGA) produced using Repli-G (Qiagen) DNA")
      .put("X", "Whole Genome Amplification (WGA) produced using Repli-G X (Qiagen) DNA (2nd Reaction)")
      .build();

  public static String convertAnalyteCode(String code) {
    return ANALYTE_CODE_MAPPING.get(code);
  }

  public static String convertSampleTypeCode(String code) {
    return SAMPLE_TYPE_CODE_MAPPING.get(code);
  }

}
