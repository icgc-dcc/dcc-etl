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
package org.icgc.dcc.etl.exporter.test.hbase;

import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_CLIENT_PORT;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;

@Slf4j
@RequiredArgsConstructor
public class EmbeddedHBase {

  /**
   * Configuration.
   */
  private final String zkConnect;

  /**
   * State.
   */
  private HBaseTestingUtility utility;

  public void startUp() throws Exception {
    val config = createConfiguration();
    utility = new HBaseTestingUtility(config);

    log.info("Starting mini-cluster...");
    utility.startMiniCluster();
    log.info("Finished starting mini-cluster");
  }

  public void shutDown() throws Exception {
    log.info("Shutting down mini-cluster...");
    utility.shutdownMiniCluster();
    log.info("Finished shutting down mini-cluster");
  }

  public Configuration getConfiguration() {
    return utility.getConfiguration();
  }

  private Configuration createConfiguration() {
    log.info("Creating configuation with zkConnect = '{}'", zkConnect);
    val config = HBaseConfiguration.create();
    config.set("test." + ZOOKEEPER_CLIENT_PORT, getZkClientPort());

    return config;
  }

  private String getZkClientPort() {
    return zkConnect.split(":")[1];
  }

}
