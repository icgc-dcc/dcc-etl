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
package org.icgc.dcc.etl.exporter;

import java.io.File;
import java.io.IOException;
import java.security.CodeSource;
import java.util.Map;
import java.util.Properties;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.tools.pigstats.ScriptState;
import org.icgc.dcc.downloader.core.SchemaUtil;
import org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage;
import org.icgc.dcc.etl.exporter.test.hbase.EmbeddedHBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Maps;

/**
 * Integration Test for Dynamic Download
 * 
 */

@Slf4j
public class EmbeddedDynamicExporter {

  /**
   * Constants.
   */
  static final String ZK_CONNECT = "localhost:21812";

  /**
   * Configuration.
   */
  File loaderDir;

  static EmbeddedHBase hbase;

  final private static String LIB_PATH = StaticMultiStorage.class.getProtectionDomain().getCodeSource().getLocation()
      .getPath();

  private final static String RELEASE = "r5";

  /**
   * State.
   */
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @BeforeClass
  @SneakyThrows
  public static void setup() throws IOException {

    hbase = new EmbeddedHBase(ZK_CONNECT);

    log.info("> Starting embedded HBase...\n");
    hbase.startUp();
    log.info("< Started embedded HBase");

    Configuration conf = hbase.getConfiguration();
    SchemaUtil.createMetaTable("meta", conf, false);
    SchemaUtil.createArchiveTable(conf, false);

  }

  @AfterClass
  @SneakyThrows
  public static void teardown() {
    hbase.shutDown();
  }

  @SneakyThrows
  protected void exportToHBase(String exporter, String observation) {
    Configuration conf = hbase.getConfiguration();
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    // PigServer pigServer = new PigServer(ExecType.LOCAL );
    System.out.println(conf.get("hbase.zookeeper.property.clientPort"));
    PigServer pigServer = new PigServer(ExecType.LOCAL, conf);
    try {
      Map<String, String> params = Maps.newHashMap();
      CodeSource extSource = StaticMultiStorage.class.getProtectionDomain().getCodeSource();
      if (extSource != null) params.put("LIB", extSource.getLocation().getPath());

      params.put("RELEASE_OUT", RELEASE);
      params.put("COMPRESSION_ENABLED ", "false");
      String tmpDirDynamic = tmp.getRoot() + "/dynamic/" + exporter;
      String tmpDirHFile = tmp.getRoot() + "/hfile/" + exporter;
      params.put("TMP_DYNAMIC_DIR", tmpDirDynamic);
      params.put("TMP_HFILE_DIR", tmpDirHFile);
      params.put("JSON_LOADER", "com.twitter.elephantbird.pig.load.JsonLoader");
      params.put("OBSERVATION", observation);
      params.put("RAW_STORAGE", "PigStorage");
      params.put("DEFAULT_PARALLEL", "1");

      Properties pigProperty = pigServer.getPigContext().getProperties();
      pigProperty.setProperty("pig.import.search.path", LIB_PATH + "/" + exporter);
      // ScriptState.start("", pigServer.getPigContext());
      ScriptState.get().setPigContext(pigServer.getPigContext());
      SchemaUtil.createDataTable(exporter, conf, false);
      pigServer.registerScript(LIB_PATH + "/" + exporter + "/dynamic.pig", params);

      @Cleanup
      HTable table = new HTable(conf, exporter);
      loader.doBulkLoad(new Path("file://" + tmpDirHFile), table);

    } finally {
      pigServer.shutdown();
    }

  }
}
