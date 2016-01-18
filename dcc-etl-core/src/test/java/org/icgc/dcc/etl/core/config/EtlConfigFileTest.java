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

import java.io.File;

import lombok.val;

import org.junit.After;
import org.junit.Test;

public class EtlConfigFileTest {

  /**
   * Test configuration file.
   */
  private static final String TEST_CONFIG_FILE = "src/test/conf/config.yaml";

  @After
  public void tearDown() {
    System.clearProperty(getOverrideName());
  }

  @Test
  public void testConfigOverride() {
    val overrideName = getOverrideName();
    val overrideValue = "override-" + System.currentTimeMillis();
    System.setProperty(overrideName, overrideValue);

    EtlConfig config = new EtlConfigFile(new File(TEST_CONFIG_FILE)).read();

    assertThat(config.getEsUri()).isEqualTo(overrideValue);
    assertThat(config.isFilterAllControlled()).isTrue();
  }

  private String getOverrideName() {
    return EtlConfigFile.PROPERTY_PREFIX + "esUri";
  }

}
