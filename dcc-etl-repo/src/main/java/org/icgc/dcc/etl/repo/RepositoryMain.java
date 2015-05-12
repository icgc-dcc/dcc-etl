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
package org.icgc.dcc.etl.repo;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.readLines;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.icgc.dcc.common.core.util.VersionUtils.getScmInfo;

import java.io.File;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.core.config.EtlConfig;
import org.icgc.dcc.etl.core.config.EtlConfigFile;
import org.icgc.dcc.etl.repo.cli.Options;
import org.icgc.dcc.etl.repo.core.RepositoryFileContextBuilder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.mongodb.MongoClientURI;

/**
 * Entry point for the {@link DBImporter}.
 */
@Slf4j
public class RepositoryMain {

  /**
   * Constants.
   */
  public static final String APPLICATION_NAME = "dcc-etl-repo";
  public static final int SUCCESS_STATUS_CODE = 0;
  public static final int FAILURE_STATUS_CODE = 1;

  /**
   * Entry point into the application.
   * 
   * @param args command line arguments
   */
  public static void main(String... args) {

    val options = new Options();
    val cli = new JCommander(options);
    cli.setAcceptUnknownOptions(true);
    cli.setProgramName(APPLICATION_NAME);

    try {
      cli.parse(args);
      run(options);
      System.exit(SUCCESS_STATUS_CODE);
    } catch (ParameterException e) {
      System.err.println("Missing parameters: " + e.getMessage());
      usage(cli);
      System.exit(FAILURE_STATUS_CODE);
    } catch (Exception e) {
      log.error("Unknown error: ", e);
      System.err.println("An an error occurred while processing. Please check the log for detailed error messages: "
          + e.getMessage());
      System.exit(FAILURE_STATUS_CODE);
    }
  }

  @SneakyThrows
  private static void run(Options options) {
    val configFilePath = options.configFilePath;
    EtlConfig config = EtlConfigFile.read(new File(configFilePath));

    logBanner(config);

    log.info("         sources        - {}", options.sources);
    log.info("         config file    - {}", options.configFilePath);
    log.info("         gene mongo uri - {}", config.getGeneMongoUri());
    log.info("         repo mongo uri - {}", config.getRepoMongoUri());

    val sources = options.sources;
    val geneMongoUri = new MongoClientURI(config.getGeneMongoUri());
    val repoMongoUri = new MongoClientURI(config.getRepoMongoUri());
    val esUri = config.getEsUri();
    val context = RepositoryFileContextBuilder.builder()
        .geneMongoUri(geneMongoUri)
        .repoMongoUri(repoMongoUri)
        .esUri(esUri)
        .realIds(true)
        .build();

    val importer = new RepositoryImporter(context);

    // Business method
    importer.execute(sources);
  }

  private static void usage(JCommander cli) {
    cli.usage();
  }

  @SneakyThrows
  private static void logBanner(EtlConfig config) {
    log.info("{}", repeat("-", 100));
    for (String line : readLines(getResource("banner.txt"), UTF_8)) {
      log.info(line);
    }
    log.info("{}", repeat("-", 100));
    log.info("Version: {}", getVersion());
    log.info("Built:   {}", getBuildTimestamp());
    log.info("SCM:");
    for (val entry : getScmInfo().entrySet()) {
      val key = entry.getKey();
      val value = firstNonNull(entry.getValue(), "").replaceAll("\n", " ");

      log.info("         {}: {}", padEnd(key, 24, ' '), value);
    }

    log.info("Config:  {}", config);
  }

  private static String getVersion() {
    return firstNonNull(RepositoryMain.class.getPackage().getImplementationVersion(), "[unknown version]");
  }

  private static String getBuildTimestamp() {
    return firstNonNull(RepositoryMain.class.getPackage().getSpecificationVersion(), "[unknown build timestamp]");
  }

}
