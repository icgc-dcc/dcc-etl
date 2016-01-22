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
import static org.icgc.dcc.etl.annotator.model.ParseNotification.WARNING_TRANSCRIPT_INCOMPLETE;
import static org.icgc.dcc.etl.annotator.model.ParseNotification.WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS;
import static org.icgc.dcc.etl.annotator.model.ParseNotification.WARNING_TRANSCRIPT_NO_START_CODON;
import static org.icgc.dcc.etl.annotator.parser.SnpEffectParser.MALFORMED_SNP_EFFECT;
import static org.icgc.dcc.etl.annotator.parser.SnpEffectParser.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.junit.Test;

@Slf4j
public class SnpEffectParserTest {

  private static final File FIXTURE_DIR = new File("src/test/resources/fixtures");
  private static final File METADATA_FILE = new File(FIXTURE_DIR, "snpEff_metadata.txt");
  private static final String COMMENT = "#";

  @Test
  @SneakyThrows
  public void parseTest() {

    val reader = new BufferedReader(new FileReader(METADATA_FILE));

    String effectAnnotation = null;
    while ((effectAnnotation = reader.readLine()) != null) {
      if (effectAnnotation.startsWith(COMMENT)) continue;
      for (val effect : parse(effectAnnotation)) {
        log.info("Created new effect: {}", effect);
        // TODO: check with Junjun if effect with the following warnings should be skipped.
        if (effect.containsAnyError(WARNING_TRANSCRIPT_NO_START_CODON, WARNING_TRANSCRIPT_INCOMPLETE,
            WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS)) {
          continue;
        }

        assertThat(effect.hasError()).isFalse();
      }
    }

    reader.close();
  }

  @Test
  public void parseTest_manualInput() {
    assertThat(parse(null)).hasSize(1).contains(MALFORMED_SNP_EFFECT);
    assertThat(parse("")).hasSize(1).contains(MALFORMED_SNP_EFFECT);
    assertThat(parse("Abc")).hasSize(1).contains(MALFORMED_SNP_EFFECT);
    assertThat(parse("name(metadata)(metadata)")).hasSize(1).contains(MALFORMED_SNP_EFFECT);
    assertThat(parse("name((metadata))")).hasSize(1).contains(MALFORMED_SNP_EFFECT);
    assertThat(parse("name()")).hasSize(1).contains(MALFORMED_SNP_EFFECT);
    assertThat(parse("name(1|2|3)")).hasSize(1).contains(MALFORMED_SNP_EFFECT);
  }

}
