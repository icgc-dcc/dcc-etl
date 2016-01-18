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
package org.icgc.dcc.etl.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl.annotator.parser.AminoAcidChangeParser.AMINO_ACID_POSITION_CALCULATION_FAILED;
import static org.icgc.dcc.etl.annotator.parser.AminoAcidChangeParser.calculateAminoAcidPosition;
import static org.icgc.dcc.etl.annotator.parser.AminoAcidChangeParser.convertToFrameshiftVariant;
import static org.icgc.dcc.etl.annotator.parser.AminoAcidChangeParser.getAminoAcidChange;
import static org.icgc.dcc.etl.annotator.parser.AminoAcidChangeParser.standardize;

import org.junit.Test;

public class AminoAcidChangeParserTest {

  @Test
  public void standardizeTest() {
    assertThat(standardize(null)).isNull();
    assertThat(standardize("")).isEmpty();
    assertThat(standardize("123")).isEqualTo("123");
    assertThat(standardize("-123")).isEqualTo("-123");
    assertThat(standardize("A123")).isEqualTo("A123A");
    assertThat(standardize("A123A")).isEqualTo("A123A");
    assertThat(standardize("123A")).isEqualTo("123A");
    assertThat(standardize("*123")).isEqualTo("*123*");
    assertThat(standardize("*123*")).isEqualTo("*123*");
    assertThat(standardize("123*")).isEqualTo("123*");
  }

  @Test
  public void getAminoAcidChangeTest() {
    assertThat(getAminoAcidChange(null)).isNull();
    assertThat(getAminoAcidChange(new String[10])).isNull();
    assertThat(getAminoAcidChange(new String[14])).isNull();
    assertThat(getAminoAcidChange(new String[] { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" }))
        .isEqualTo("3");
  }

  @Test
  public void convertToFrameshiftVariantTest() {
    // Does not start with '-'
    assertThat(convertToFrameshiftVariant("*123")).isEqualTo("*123");
    // Does not start with '-'
    assertThat(convertToFrameshiftVariant("R123")).isEqualTo("R123");
    // Valid
    assertThat(convertToFrameshiftVariant("-R123")).isEqualTo("X123fs");
    // Valid
    assertThat(convertToFrameshiftVariant("-123")).isEqualTo("X123fs");
    // Can't calculate aaChange position
    assertThat(convertToFrameshiftVariant("-123-123")).isEqualTo("-123-123");
  }

  @Test
  public void calculateAminoAcidPositionTest() {
    assertThat(calculateAminoAcidPosition("123")).isEqualTo(122L);
    assertThat(calculateAminoAcidPosition("R123R123")).isEqualTo(AMINO_ACID_POSITION_CALCULATION_FAILED);
  }

}
