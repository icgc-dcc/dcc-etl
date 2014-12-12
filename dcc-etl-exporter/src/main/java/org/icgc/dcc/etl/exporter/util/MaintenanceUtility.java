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
package org.icgc.dcc.etl.exporter.util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest.CompactionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.icgc.dcc.downloader.core.SchemaUtil;

import com.google.common.collect.ImmutableList;

@Slf4j
public class MaintenanceUtility {

  private static void runCompact(String releaseName, String dataType, boolean major) throws IOException,
      InterruptedException {
    HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
    try {
      // for each table go through each region
      HTableDescriptor[] tables = admin.listTables(SchemaUtil.getDataTableName(dataType, releaseName));
      for (HTableDescriptor table : tables) {
        log.info("-- compacting table: {}", table.getNameAsString());

        try {
          if (major) {
            admin.majorCompact(table.getName());
          } else {
            admin.compact(table.getName());
          }
          int n = 0;
          do {
            wait(n);
            n++;
          } while (isCompactionDone(admin, table.getName()));
        } catch (IOException e) {
          log.warn("Table is not valid anymore: {}", table.getNameAsString(), e);
        }
      }
    } finally {
      admin.close();
    }
  }

  private static boolean isCompactionDone(HBaseAdmin admin, byte[] name) throws IOException,
      InterruptedException {
    return admin.getCompactionState(name) != CompactionState.NONE;
  }

  private static int[] TimeDelayInMilli =
      new int[] { 15000, 1000, 1500, 1600, 1600, 1800, 2400, 3000, 4000, 5000, 8000, 10000, 14000, 20000 };

  private static void wait(int retries) throws InterruptedException {
    if (retries < TimeDelayInMilli.length) {
      TimeUnit.MILLISECONDS.sleep(TimeDelayInMilli[retries]);
    } else {
      TimeUnit.MILLISECONDS.sleep(20000);
    }
  }

  public static void main(String[] argv) throws IOException, InterruptedException {
    String releaseName = argv[0];

    if (argv.length == 4 && Boolean.valueOf(argv[1])) {
      balancer(releaseName);
    }

    boolean major = false;
    if (argv.length == 2) {
      major = Boolean.valueOf(argv[1]);
    }
    String dataType = ".*.";
    if (argv.length == 3) {
      dataType = argv[1];
    }
    runCompact(releaseName, dataType, major);
  }

  private static void balancer(String releaseName) throws IOException, InterruptedException {
    HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
    try {
      HTableDescriptor[] tables = admin.listTables(SchemaUtil.getDataTableName(".*.", releaseName));
      for (HTableDescriptor table : tables) {
        log.info("-- Balancing table: {}", table.getNameAsString());

        List<HRegionInfo> regionInfo = admin.getTableRegions(table.getName());
        List<ServerName> servers = ImmutableList.copyOf(admin.getClusterStatus().getServers());
        for (int i = 0; i < regionInfo.size(); ++i) {
          HRegionInfo region = regionInfo.get(i);
          if (!region.isOffline()) {
            ServerName server = servers.get(i % servers.size());
            String serverName = server.getServerName();

            String regionEncodedName = regionInfo.get(i).getEncodedName();
            log.info("Moving region: " + regionEncodedName + " to : " + serverName);
            try {
              admin.move(Bytes.toBytes(regionEncodedName), Bytes.toBytes(serverName));
            } catch (IOException e) {
              log.warn("fail to move region: {} ", regionEncodedName, e);
            }
          }
        }
      }
    } finally {
      admin.close();
    }
  }
}
