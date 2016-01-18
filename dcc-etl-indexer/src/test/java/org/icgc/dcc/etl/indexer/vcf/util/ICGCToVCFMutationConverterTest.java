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
package org.icgc.dcc.etl.indexer.vcf.util;

import static com.google.common.collect.ImmutableList.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl.indexer.vcf.model.MutationType.DELETION;
import static org.icgc.dcc.etl.indexer.vcf.model.MutationType.INSERTION;
import static org.icgc.dcc.etl.indexer.vcf.model.MutationType.SINGLE_BASE_SUBSTITUTION;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;

import lombok.RequiredArgsConstructor;
import lombok.val;
import net.sf.picard.reference.IndexedFastaSequenceFile;

import org.icgc.dcc.etl.indexer.vcf.model.MutationType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;

@RunWith(value = Parameterized.class)
@RequiredArgsConstructor
public class ICGCToVCFMutationConverterTest {

  /**
   * Test config.
   */
  private static final File REFERENCE_GENOME = new File("/tmp/GRCh37.fasta");

  /**
   * Class under test.
   */
  ICGCToVCFMutationConverter converter;

  /**
   * Input
   */
  final String chromosome;
  final long start;
  final long end;
  final String mutation;
  final MutationType type;
  final String reference;

  /**
   * Expected
   */
  final VCFMutation expected;

  @Before
  public void setUp() throws FileNotFoundException {
    this.converter = new ICGCToVCFMutationConverter(new IndexedFastaSequenceFile(REFERENCE_GENOME));
  }

  /**
   * Test data.
   * <p>
   * @see https://wiki.oicr.on.ca/display/DCCSOFT/Aggregated+Data+Download+Specification
   */
  @Parameters
  public static Collection<Object[]> data() {
    // TODO: add remaining tests
    return ImmutableList.copyOf(new Object[][] {
        { "6", 123456L, 123458L, "TTC>-", DELETION, "TTC", new VCFMutation(123455L, of("A"), "ATTC", "ATTC>A") },
        { "6", 1L, 1L, "N>-", DELETION, "N", new VCFMutation(1L, of("N"), "NN", "NN>N") },
        { "9", 100000L, 1000L, "->A", INSERTION, "-", new VCFMutation(99999L, of("CA"), "C", "C>CA") },
        { "9", 1L, 1L, "N>A", INSERTION, "N", new VCFMutation(1L, of("AN"), "N", "N>AN") },
        { "6", 123456L, 123456L, "C>T", SINGLE_BASE_SUBSTITUTION, "T", new VCFMutation(123456L, of("C"), "T", "C>T") },
        { "6", 123456L, 123456L, "A>T", SINGLE_BASE_SUBSTITUTION, "A", new VCFMutation(123456L, of("T"), "A", "A>T") },
    });
  }

  @Test
  public void testConvert() {
    val actual = converter.convert(chromosome, start, end, mutation, type, reference);

    assertThat(actual).isEqualTo(expected);
  }

}
