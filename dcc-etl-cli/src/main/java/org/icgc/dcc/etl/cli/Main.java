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
package org.icgc.dcc.etl.cli;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Optional.fromNullable;
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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.LogManager;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.config.EtlConfig;
import org.icgc.dcc.etl.config.EtlConfigFile;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.icgc.dcc.etl.service.EtlService;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public class Main {

  /**
   * Return code to indicate to the OS that a fatal application error occurred.
   */
  private static final int ERROR_RETURN_CODE = 1;

  /**
   * Command line options.
   */
  private final Options options = new Options();

  static {
    // Initialize j.u.l to Slf4j because of dcc-icgc-client.
    // See https://github.com/icgc-dcc/dcc/blob/develop/dcc-icgc-client/README.md
    LogManager.getLogManager().reset();
    SLF4JBridgeHandler.install();
  }

  /**
   * Entry point into the application.
   * 
   * @param args command line arguments
   */
  public static void main(String... args) {
    new Main().run(args);
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
        out.printf("ICGC DCC ETL%nVersion %s%n", getVersion());

        for (Entry<String, String> entry : getScmInfo().entrySet()) {
          String key = entry.getKey();
          String value = firstNonNull(entry.getValue(), "").replaceAll("\n", " ");

          out.printf("  %s: %s%n", padEnd(key, 24, ' '), value);
        }

        return;
      }

      EtlConfig config = read(options.config);
      logBanner(args, config);
      execute(config);
    } catch (ParameterException pe) {
      err.printf("dcc-etl: %s%n", pe.getMessage());
      err.printf("Try '%s --help' for more information.%n", getProgramName());
    } catch (Throwable t) {
      log.error("Error running release '" + options.releasePrefix + "." + options.releaseNumber + "' ETL: ", t);

      exit(ERROR_RETURN_CODE);
    }
  }

  @SneakyThrows
  private void logBanner(String[] args, EtlConfig config) {
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
    log.info("Options: processes      - {}", options.processes.isEmpty() ? "all" : options.processes);
    log.info("         dictionary     - {}", options.dictionaryFilePath);
    log.info("         codelists      - {}", options.codeListsFilePath);
    log.info("         job ID         - {}", options.jobId);
    log.info("         release prefix - {}", options.releasePrefix);
    log.info("         release number - {}", options.releaseNumber);
    log.info("         patch number   - {}", options.patchNumber);
    log.info("         run number     - {}", options.runNumber);
    log.info("         projects       - {}", options.projects);
    log.info("         working dir    - {}", options.workingDir);
    log.info("         index-types    - {}", options.types.isEmpty() ? "all" : options.types);
    log.info("         config file    - {}", options.config.getAbsolutePath());
    log.info("Config:  {}", config);
  }

  @SneakyThrows
  private EtlConfig read(File file) {
    return new EtlConfigFile(file).read();
  }

  private void execute(EtlConfig config) {
    log.info("Executing ETL...\n");
    EtlService service = new EtlService(config);
    service.execute(
        options.jobId,
        options.releasePrefix,
        options.releaseNumber,
        options.patchNumber,
        options.runNumber,
        options.dictionaryFilePath,
        options.codeListsFilePath,
        ImmutableSet.copyOf(options.projects), // JCommander doesn't seem to support Sets directly
        options.workingDir,
        options.processes,
        options.types.isEmpty() ?
            ImmutableList.copyOf(DocumentType.values()) :
            options.types,
        fromNullable(options.alias));
    log.info("Finished ETL!\n");
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
