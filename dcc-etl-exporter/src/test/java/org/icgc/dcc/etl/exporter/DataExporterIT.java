package org.icgc.dcc.etl.exporter;

import java.io.File;
import java.io.IOException;
import java.security.CodeSource;
import java.util.Map;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.icgc.dcc.etl.exporter.pig.udf.CreateIndex;
import org.icgc.dcc.etl.exporter.pig.udf.DataDownloadStorage;
import org.icgc.dcc.etl.exporter.pig.udf.DynamicDownloadExporter;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * driver for data exporter
 * 
 */

@Slf4j
public class DataExporterIT {

  private String outDirStatic;

  private final static String RELEASE = "r5";

  private final static String ROOT = "/tmp/download";

  private final static String TMP_DIR = FilenameUtils.concat(ROOT, "tmp");

  private final static String ROOT_OUT_STATIC = FilenameUtils.concat(ROOT, "static");

  private final static String DS_ROOT = "/tmp/etl/dcc-etl-cli";

  private final static String ROOT_OUT_INDEX = ROOT + "/dynamic" + "/" + RELEASE;

  private final static String ROOT_OUT_VALIDATION = ROOT + "/validation/" + RELEASE;

  private final static String ROOT_TMP_INDEX = TMP_DIR + "/dataindex/" + RELEASE;

  private final static String DATA_INDEXER = "dataindexer.pig";

  private final static Map<String, String> exporters = ImmutableMap.<String, String> builder()
      .put("ssmopenexporter", "ssm")
      .put("ssmcontrolledexporter", "ssm")
      .put("clinicalexporter", "donor")
      .put("clinicalsampleexporter", "donor")

      // we don't have those data types yet for testing
      // .put("cnsmexporter", "cnsm.json")

      // .put("jcnexporter", "jcn.json")
      // .put("stsmexporter", "stsm.json")

      // .put("pexpexporter", "pexp.json")
      // .put("expexporter", "exp.json")
      .build();

  @Before
  public void setup() throws IOException {
    this.outDirStatic = ROOT_OUT_STATIC + "/" + RELEASE;

    FileUtils.deleteDirectory(new File(ROOT));
  }

  @SneakyThrows
  @Test
  @Ignore
  public void createDataDownload() {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    try {

      Map<String, String> params = Maps.newHashMap();
      CodeSource extSource = DataDownloadStorage.class.getProtectionDomain().getCodeSource();
      if (extSource != null) params.put("LIB", extSource.getLocation().getPath());

      params.put("RELEASE_OUT", RELEASE);
      params.put("EMPTY_VALUE", "-");
      for (String exporter : exporters.keySet()) {

        String tmpDirStatic = TMP_DIR + "/" + exporter + "_static";
        String tmpDirDynamic = TMP_DIR + "/dynamic/" + exporter;
        String datasource = DS_ROOT + "/" + exporters.get(exporter);

        params.put("TMP_STATIC_DIR", tmpDirStatic);
        params.put("OUT_STATIC_DIR", outDirStatic);
        params.put("TMP_DYNAMIC_DIR", tmpDirDynamic);

        params.put("OBSERVATION", datasource);

        fs.delete(new Path(tmpDirStatic), true);
        fs.delete(new Path(tmpDirDynamic), true);

        fs.mkdirs(new Path(outDirStatic));
        String file = DataDownloadStorage.class.getResource("/scripts/" + exporter + ".pig").toURI().toURL()
            .getFile();
        log.info("Registering: {}", file);
        pigServer.registerScript(file, params);
      }

      fs.delete(new Path(ROOT_TMP_INDEX), true);
      params.put("PARTIAL_INDEX", TMP_DIR + "/dynamic/*/part-r-*");
      params.put("INDEX_DIR", ROOT_TMP_INDEX);
      pigServer.registerScript(CreateIndex.class.getResource("/scripts/" + DATA_INDEXER).toURI().toURL().getFile(),
          params);

      fs.delete(new Path(ROOT_OUT_INDEX), true);
      DynamicDownloadExporter.run(ROOT_TMP_INDEX, ROOT_OUT_INDEX, ROOT_OUT_VALIDATION, 1024);
      // FileUtils.moveFile(new File(FilenameUtils.concat(ROOT_TMP_INDEX, REDUCE_OUT_DIR)), new File(
      // ROOT_OUT_INDEX));

    } catch (Exception e) {
      log.error("fail to produce data", e);
    } finally {
      fs.close();
      pigServer.shutdown();
    }
  }
}
