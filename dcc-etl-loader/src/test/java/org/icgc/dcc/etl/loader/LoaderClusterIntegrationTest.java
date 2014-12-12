/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.loader;

import static com.google.common.collect.ImmutableMap.of;
import static org.icgc.dcc.common.hadoop.util.HadoopConstants.MAPRED_COMPRESSION_MAP_OUTPUT_PROPERTY_NAME;
import lombok.SneakyThrows;

import org.icgc.dcc.common.hadoop.util.HadoopConstants;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Integration test to exercise the loader main entry point using a pseudo-distributed cluster.
 * <p>
 * TODO: Move hadoop constants to {@link HadoopConstants}.
 */
public class LoaderClusterIntegrationTest extends BaseClusterTest {

  private final LoaderIntegrationTestHelper helper = new LoaderIntegrationTestHelper();

  /**
   * Simulates the state of a clean release with a single validated project.
   */
  @Override
  @SneakyThrows
  public void setUp() {
    super.setUp();

    helper.setFsUrl(fsUrl);
    helper.setHadoop(of(
        "fs.defaultFS", fsUrl,
        "mapred.job.tracker", jobTracker,
        "mapred.job.shuffle.input.buffer.percent", "0.5",
        MAPRED_COMPRESSION_MAP_OUTPUT_PROPERTY_NAME, "false"));
    helper.setUp();

  }

  /**
   * Execute the loader locally on an integration test data set and compares the results to verified set of output
   * files.
   */
  @Test
  @Ignore
  public void testLocalLoader() {
    helper.testLoader("cluster");
  }

}
