/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
import static org.icgc.dcc.etl.annotator.parser.ConsequenceTypeParser.parse;

import org.icgc.dcc.etl.annotator.model.ConsequenceType;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ConsequenceTypeParserTest {

  @Test
  public void parseTest() {
    assertThat(parse("5_codon")).isEqualTo(ImmutableList.of("5_codon"));
    assertThat(parse("5_codon+exon_change")).isEqualTo(ImmutableList.of("5_codon", "exon_change"));
  }

  @Test(expected = IllegalStateException.class)
  public void incorrectEffectsQuantityTest() {
    parse("5_codon+exon_change+extra_effect");
  }

  @Test(expected = IllegalStateException.class)
  public void emptyArgumentTest() {
    parse("");
  }

  @Test(expected = IllegalStateException.class)
  public void nullArgumentTest() {
    parse(null);
  }

  @Test
  public void exonLossTest() {
    assertThat(parse("3_prime_UTR_truncation+exon_loss")).isEqualTo(
        ImmutableList.of("3_prime_UTR_truncation", ConsequenceType.EXON_LOSS_VARIANT.getId()));
  }

}
