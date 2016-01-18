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
package org.icgc.dcc.etl.annotator.converter;

import static com.google.common.collect.ImmutableList.of;
import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;

import org.broadinstitute.variant.variantcontext.VariantContextBuilder;
import org.icgc.dcc.etl.annotator.model.AnnotatedFileType;
import org.icgc.dcc.etl.annotator.util.Alleles;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

@Ignore
public class SnpEffVCFToICGCConverterTest {

  /**
   * Class under test.
   */
  SnpEffVCFToICGCConverter converter = new SnpEffVCFToICGCConverter("75");

  // TODO: Create tests to cover all cases
  @Test
  public void testConvert() {
    val variant = new VariantContextBuilder()
        .chr("1")
        .start(1)
        .stop(1)
        .id("MU00001")
        // .genotypes(Genotypes)
        .attributes(ImmutableMap.<String, Object> of(SnpEffVCFToICGCConverter.INFO_EFF_FIELD, ""))
        .alleles(Alleles.createAlleles("A", of("T")))
        .make();

    val records = converter.convert(variant, AnnotatedFileType.SSM);

    // TODO: Verify results
    assertThat(records).isNotNull();
  }

}
