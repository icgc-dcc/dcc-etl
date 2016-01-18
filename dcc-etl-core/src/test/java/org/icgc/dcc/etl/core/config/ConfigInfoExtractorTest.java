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
package org.icgc.dcc.etl.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.Optionals.ABSENT_STRING;
import static org.icgc.dcc.common.test.Tests.LOCALHOST;
import static org.icgc.dcc.etl.core.config.ConfigInfoExtractor.getInfo2;

import java.io.File;

import org.icgc.dcc.etl.core.config.ConfigInfoExtractor.SubType;
import org.icgc.dcc.etl.core.config.ConfigInfoExtractor.Type;
import org.junit.Test;

import com.google.common.base.Optional;

public class ConfigInfoExtractorTest {

  private static String TEST_CONFIG_FILE_PATH = "src/test/conf/config.yaml";
  private static final File TEST_CONFIG_FILE = new File(TEST_CONFIG_FILE_PATH);

  @Test
  public void test_config_info_extractor() {
    assertThat(getInfo2(
        TEST_CONFIG_FILE,
        Type.MONGO_ETL_ADMIN,
        SubType.USERNAME))
        .isEqualTo(ABSENT_STRING);
    assertThat(getInfo2(
        TEST_CONFIG_FILE,
        Type.MONGO_ETL_ADMIN,
        SubType.HOST))
        .isEqualTo(Optional.of(LOCALHOST));
    assertThat(getInfo2(
        TEST_CONFIG_FILE,
        Type.NAMENODE,
        SubType.HOST))
        .isEqualTo(ABSENT_STRING);
    assertThat(getInfo2(
        TEST_CONFIG_FILE,
        Type.NAMENODE,
        SubType.HOST))
        .isEqualTo(ABSENT_STRING);
    assertThat(getInfo2(
        TEST_CONFIG_FILE,
        Type.JOBTRACKER,
        SubType.HOST))
        .isEqualTo(Optional.of(LOCALHOST));
  }
}
