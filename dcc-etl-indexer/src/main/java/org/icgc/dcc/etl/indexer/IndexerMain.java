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
package org.icgc.dcc.etl.indexer;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.readLines;
import static java.lang.System.err;
import static java.lang.System.exit;
import static java.lang.System.out;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.repeat;

import static org.icgc.dcc.common.core.util.VersionUtils.getScmInfo;
import static org.icgc.dcc.etl.indexer.factory.DocumentServiceFactory.newDistributedService;
import static org.icgc.dcc.etl.indexer.factory.DocumentServiceFactory.newLocalService;
import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map.Entry;

import lombok.Cleanup;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.indexer.cli.Options;
import org.icgc.dcc.etl.indexer.core.Config;
import org.icgc.dcc.etl.indexer.core.DocumentService;
import org.icgc.dcc.etl.indexer.report.DocumentTypeReport;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * Application entry point.
 */
@Slf4j
@NoArgsConstructor(access = PRIVATE)
public class IndexerMain {

  /**
   * Return code to indicate to the OS that a fatal application error occurred.
   */
  private static final int ERROR_RETURN_CODE = 1;

  /**
   * Command line options.
   */
  private final Options options = new Options();

  /**
   * Entry point into the application.
   * 
   * @param args command line arguments
   */
  public static void main(String... args) {
    new IndexerMain().run(args);
  }

  @SneakyThrows
  private void run(String... args) {
    JCommander cli = new JCommander(options);
    cli.setProgramName(getProgramName());

    try {
      cli.parse(args);

      if (options.help) {
        cli.usage();

        return;
      } else if (options.version) {
        out.printf("ICGC DCC ETL Indexer%nVersion %s%n", getVersion());

        for (Entry<String, String> entry : getScmInfo().entrySet()) {
          String key = entry.getKey();
          String value = firstNonNull(entry.getValue(), "").replaceAll("\n", " ");

          out.printf("  %s: %s%n", padEnd(key, 24, ' '), value);
        }

        return;
      }

      logBanner(args);
      execute();
    } catch (ParameterException pe) {
      err.printf("dcc-etl-indexer: %s%n", pe.getMessage());
      err.printf("Try '%s --help' for more information.%n", getProgramName());
    } catch (Throwable t) {
      log.error("Error running '" + options + "': ", t);

      exit(ERROR_RETURN_CODE);
    }

    log.info("Exiting...\n\n");
  }

  @SneakyThrows
  private void logBanner(String[] args) {
    log.info("{}", repeat("-", 100));
    for (String line : readLines(getResource("banner.txt"), UTF_8)) {
      log.info(line);
    }
    log.info("{}", repeat("-", 100));
    log.info("Version: {}", getVersion());
    log.info("Built:   {}", getBuildTimestamp());
    log.info("SCM:");
    for (Entry<String, String> entry : getScmInfo().entrySet()) {
      String key = entry.getKey();
      String value = firstNonNull(entry.getValue(), "").replaceAll("\n", " ");

      log.info("         {}: {}", padEnd(key, 24, ' '), value);
    }

    log.info("Command: {}", formatArguments(args));
    log.info("Options: releaseName  - {}", options.releaseName);
    log.info("         indexName    - {}", options.indexName);
    log.info("         types        - {}", options.types);
    log.info("         fastaFile    - {}", options.fastaFile);
    log.info("         outputDir    - {}", options.outputDir);
    log.info("         optimize     - {}", options.optimize);
    log.info("         hadoop       - {}", options.hadoop);
    log.info("         {}\n", options);
  }

  private void execute() throws IOException {
    // Create the service
    Config config = Config.builder()
        .releaseName(options.releaseName)
        .mongoUri(getMongoUri(options.mongoUri, options.databaseName))
        .fsUri(firstNonNull(options.hadoop.get("fs.defaultFS"), "file:///"))
        .esUri(options.esUri)
        .indexName(options.indexName)
        .fastaFile(options.fastaFile)
        .outputDir(options.outputDir)
        .exportVCF(options.exportVCF)
        .optimize(options.optimize)
        .hadoop(options.hadoop)
        .build();

    @Cleanup
    DocumentService service = createService(config);

    // Execute
    log.info("Executing...");
    service.writeDocuments(options.types);
    log.info("Finished executing!");

    // Report
    val report = new DocumentTypeReport(options.indexName, newTransportClient(options.esUri));
    report.report(options.types);
  }

  private DocumentService createService(Config config) {
    return options.local ? newLocalService(config) : newDistributedService(config);
  }

  private String getMongoUri(String mongoUri, String databaseName) {
    return String.format("%s/%s", mongoUri, databaseName);
  }

  private String getProgramName() {
    return "java -jar " + getJarName();
  }

  private String getVersion() {
    return firstNonNull(getClass().getPackage().getImplementationVersion(), "[unknown version]");
  }

  private String getBuildTimestamp() {
    return firstNonNull(getClass().getPackage().getSpecificationVersion(), "[unknown build timestamp]");
  }

  private String getJarName() {
    return new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath()).getName();
  }

  private List<String> getJavaArguments() {
    return ManagementFactory.getRuntimeMXBean().getInputArguments();
  }

  private String formatArguments(String[] args) {
    return "java " + join(getJavaArguments(), ' ') + " -jar " + getJarName() + " " + join(args, ' ');
  }

}
