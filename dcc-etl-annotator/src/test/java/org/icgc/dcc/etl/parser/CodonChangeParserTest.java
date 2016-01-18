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
import static org.icgc.dcc.etl.annotator.parser.CodonChangeParser.getCodonChange;
import static org.icgc.dcc.etl.annotator.parser.CodonChangeParser.parseCodonChange;
import lombok.val;

import org.junit.Test;

public class CodonChangeParserTest {

  @Test
  public void getCodonChangeTest() {
    assertThat(getCodonChange(null)).isNull();
    assertThat(getCodonChange(new String[10])).isNull();
    assertThat(getCodonChange(new String[14])).isNull();
    assertThat(getCodonChange(new String[] { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" })).isEqualTo("2");
  }

  @Test
  public void parseCodonChangeTest_arguments() {
    val emptyString = "";
    val one = "1";

    // Empty and null arguments
    assertThat(parseCodonChange(null, null)).isEqualTo(emptyString);
    assertThat(parseCodonChange(null, one)).isEqualTo(emptyString);
    assertThat(parseCodonChange(one, null)).isEqualTo(emptyString);
    assertThat(parseCodonChange(emptyString, emptyString)).isEqualTo(emptyString);
    assertThat(parseCodonChange(emptyString, one)).isEqualTo(emptyString);
    assertThat(parseCodonChange(one, emptyString)).isEqualTo(emptyString);

    // Incorrect codonChange length
    assertThat(parseCodonChange("a/a/a", one)).isEqualTo(emptyString);
    assertThat(parseCodonChange("a/a", one)).isEqualTo(emptyString);
    assertThat(parseCodonChange("aaaa/aaaa", one)).isEqualTo(emptyString);
    assertThat(parseCodonChange("aaa/aaaa", one)).isEqualTo(emptyString);
    assertThat(parseCodonChange("aaaa/aaa", one)).isEqualTo(emptyString);
    assertThat(parseCodonChange("a|a", one)).isEqualTo(emptyString);

    // cds_mutation generation fail
    assertThat(parseCodonChange("aaa/aaa", one)).isNull();

    // Can't calculate AminoAcid position
    assertThat(parseCodonChange(one, "a123a123")).isEqualTo(emptyString);
  }

  @Test
  public void parseCodonChangeTest() {
    // Position calculation:
    // (Number from AminoAcidChange -1) * 3 + position of first baseHighlight(capital letter that indicates what
    // nucleotide changed. First nucleotide in a codon has position 0) + 1
    // E.g. "cgG/cgA", "R626" => (626 - 1) * 3 + 2 + 1 = 1875 + 2 + 1 = 1878
    // CodonChange From > To: G>A
    // Final result 1878G>A
    assertThat(parseCodonChange("cgG/cgA", "R626")).isEqualTo("1878G>A");
    assertThat(parseCodonChange("cGg/cAg", "R626")).isEqualTo("1877G>A");
    assertThat(parseCodonChange("Gcg/Acg", "R626")).isEqualTo("1876G>A");

    // (2-1) * 3 + 2 + 1 = 6
    assertThat(parseCodonChange("cgG/cgA", "R2")).isEqualTo("6G>A");
    // (2-1) * 3 + 1 + 1 = 5
    assertThat(parseCodonChange("cGg/cAg", "R2")).isEqualTo("5G>A");
    // (2-1) * 3 + 0 + 1 = 4
    assertThat(parseCodonChange("Gcg/Acg", "R2")).isEqualTo("4G>A");

    // baseHighlight is required to be in any codon pair
    assertThat(parseCodonChange("Gcg/acg", "R2")).isEqualTo("4G>A");
    assertThat(parseCodonChange("gcg/Acg", "R2")).isEqualTo("4G>A");

    // 2 offsetHighlights indicate change from GC to AC
    assertThat(parseCodonChange("GCg/acg", "R2")).isEqualTo("4GC>AC");
    // pair side does not matter
    assertThat(parseCodonChange("gcg/ACg", "R2")).isEqualTo("4GC>AC");
    // 3 nucleotide change
    assertThat(parseCodonChange("gcg/ACG", "R2")).isEqualTo("4GCG>ACG");
    assertThat(parseCodonChange("GCG/acg", "R2")).isEqualTo("4GCG>ACG");
    assertThat(parseCodonChange("GCG/ACG", "R2")).isEqualTo("4GCG>ACG");
  }

}
