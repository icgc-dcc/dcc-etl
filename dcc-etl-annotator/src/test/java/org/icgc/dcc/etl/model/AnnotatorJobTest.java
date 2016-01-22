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
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.etl.annotator.model.AnnotatorJob;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

@Slf4j
public class AnnotatorJobTest {

  @Test
  public void getPathsByProject() {
    val workingDir = "/tmp";
    val abcPath = new Path(workingDir, "ABC");
    val xyzPath = new Path(workingDir, "XYZ");
    val abcSsmPath = new Path(abcPath, "ssm_p.txt");

    val job = AnnotatorJob.builder()
        .projectNames(ImmutableList.of("ABC", "XYZ"))
        .fileTypes(ImmutableList.of(SSM, SGV))
        .workingDir(workingDir)
        .files(ImmutableList.of(abcSsmPath, new Path(xyzPath, "sgv_p.txt")))
        .build();

    val files = job.getFilesByProject("ABC");
    log.info("{}", files);
    assertThat(files).containsOnly(new Path(abcPath, "ssm_p.txt"));
  }

}
