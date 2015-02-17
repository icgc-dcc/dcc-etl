package org.icgc.dcc.etl.exporter;

import java.io.File;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.exporter.test.hbase.EmbeddedHBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
@Ignore("Nothing implemented yet. See DCC-2944 for details")
public class ExporterIntegrationTest {

  /**
   * Constants.
   */
   static final String ZK_CONNECT = "localhost:21812";
   
   /**
    * Configuration.
    */
   File loaderDir;
  
  /**
   * Collaborators.
   */
  EmbeddedHBase hbase;

  /**
   * State.
   */
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Before
  @SneakyThrows
  public void setUp() {
    this.hbase = new EmbeddedHBase(ZK_CONNECT);
    
    log.info("> Starting embedded HBase...\n");
    hbase.startUp();
    log.info("< Started embedded HBase");
    
    log.info("> Creating loader directory...\n");
    this.loaderDir = tmp.newFolder();
    log.info("< Created loader directory: {}", loaderDir);
  }

  @SneakyThrows
  @After
  public void shutDown() {
    log.info("> Stopping embedded HBase...");
    hbase.shutDown();
    hbase = null;
    log.info("< Stopped embedded HBase");
  }
  
  @Test
  public void testExporter() {
    // TODO: Implement
    log.info("Testing...");
  }

}
