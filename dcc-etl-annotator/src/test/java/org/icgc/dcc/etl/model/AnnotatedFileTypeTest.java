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
package org.icgc.dcc.etl.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl.annotator.model.AnnotatedFileType.SGV;
import static org.icgc.dcc.etl.annotator.model.AnnotatedFileType.SSM;
import static org.icgc.dcc.etl.annotator.model.AnnotatedFileType.byName;
import static org.icgc.dcc.etl.annotator.model.AnnotatedFileType.byPath;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class AnnotatedFileTypeTest {

  @Test
  public void byNameTest() {
    assertThat(byName("ssm")).isEqualTo(SSM);
    assertThat(byName("sgv")).isEqualTo(SGV);
  }

  @Test(expected = IllegalArgumentException.class)
  public void byNameUnkownTest() {
    byName("fake");
  }

  @Test
  public void getByPathTest() {
    assertThat(byPath(new Path("/tmp", "ssm_p.txt"))).isEqualTo(SSM);
    assertThat(byPath(new Path("/tmp", "sgv_p.txt"))).isEqualTo(SGV);
  }

  @Test(expected = IllegalArgumentException.class)
  public void getByPathUnknownTest() {
    byPath(new Path("/tmp", "fake.txt"));
  }

  @Test
  public void getInputFileNameTest() {
    assertThat(SSM.getInputFileName()).isEqualTo("ssm_p.txt");
    assertThat(SGV.getInputFileName()).isEqualTo("sgv_p.txt");
  }

  @Test
  public void getOutputFileNameTest() {
    assertThat(SSM.getOutputFileName()).isEqualTo("ssm_s.txt");
    assertThat(SGV.getOutputFileName()).isEqualTo("sgv_s.txt");
  }

}
