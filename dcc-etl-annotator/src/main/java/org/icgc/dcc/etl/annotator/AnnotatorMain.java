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
package org.icgc.dcc.etl.annotator;

import static com.google.common.base.Objects.firstNonNull;
import static java.lang.System.err;
import static java.lang.System.exit;
import static java.lang.System.out;
import static org.apache.commons.lang.StringUtils.join;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.List;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.annotator.cli.Options;
import org.icgc.dcc.etl.annotator.service.AnnotatorService;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * Application entry point.
 */
@Slf4j
@Configuration
@ComponentScan
public class AnnotatorMain {

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
    new AnnotatorMain().run(args);
  }

  @SneakyThrows
  private void run(String... args) {
    val cli = new JCommander(options);
    cli.setAcceptUnknownOptions(true);
    cli.setProgramName(getProgramName());

    try {
      cli.parse(args);

      if (options.help) {
        cli.usage();

        return;
      } else if (options.version) {
        out.printf("ICGC DCC Annotator%nVersion %s%n", getVersion());

        return;
      }

      execute(args);
    } catch (ParameterException pe) {
      err.printf("dcc-etl-annotator: %s%n", pe.getMessage());
      err.printf("Try '%s --help' for more information.%n", getProgramName());
    } catch (Throwable t) {
      log.error("Error running Annotator: ", t);

      exit(ERROR_RETURN_CODE);
    }
  }

  private void execute(String... args) {
    val context = new SpringApplicationBuilder().sources(AnnotatorMain.class).run(args);
    val service = context.getBean(AnnotatorService.class);

    log.info("Executing Annotator...\n");
    log.info("Command: {}", formatArguments(args));

    service.annotate(options.workingDir, options.projectNames, options.fileTypes);
    log.info("Finished Annotator!\n");
  }

  private String getProgramName() {
    return "java -jar " + getJarName();
  }

  private String getVersion() {
    return firstNonNull(getClass().getPackage().getImplementationVersion(), "[unknown version]");
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
