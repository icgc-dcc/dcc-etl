/*
 * Copyright 2013(c) The Ontario Institute for Cancer Research. All rights reserved.
 * 
 * This program and the accompanying materials are made available under the terms of the GNU Public
 * License v3.0. You should have received a copy of the GNU General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.etl.identifier;

import static com.google.common.base.Strings.padEnd;
import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.repeat;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.icgc.dcc.etl.identifier.config.IdentifierConfig;
import org.icgc.dcc.etl.identifier.mapper.BadRequestExceptionMapper;
import org.icgc.dcc.etl.identifier.mapper.NotFoundExceptionMapper;
import org.icgc.dcc.etl.identifier.repository.DonorRepository;
import org.icgc.dcc.etl.identifier.repository.MutationRepository;
import org.icgc.dcc.etl.identifier.repository.ProjectRepository;
import org.icgc.dcc.etl.identifier.repository.SampleRepository;
import org.icgc.dcc.etl.identifier.repository.SpecimenRepository;
import org.icgc.dcc.etl.identifier.resource.DonorResource;
import org.icgc.dcc.etl.identifier.resource.MutationResource;
import org.icgc.dcc.etl.identifier.resource.ProjectResource;
import org.icgc.dcc.etl.identifier.resource.SampleResource;
import org.icgc.dcc.etl.identifier.resource.SpecimenResource;
import org.icgc.dcc.etl.identifier.util.VersionUtils;
import org.skife.jdbi.v2.DBI;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import com.yammer.dropwizard.jdbi.bundles.DBIExceptionsBundle;
import com.yammer.metrics.reporting.ConsoleReporter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IdentifierMain extends Service<IdentifierConfig> {

  private static final String APPLICATION_NAME = "dcc-etl-identifier";
  private static String[] args;

  public static void main(String... args) throws Exception {
    IdentifierMain.args = args;
    new IdentifierMain().run(args);
  }

  @Override
  public final void initialize(Bootstrap<IdentifierConfig> bootstrap) {
    bootstrap.setName(APPLICATION_NAME);
    bootstrap.addBundle(new DBIExceptionsBundle());
  }

  @Override
  public final void run(IdentifierConfig config, Environment environment) throws Exception {
    DBI jdbi = createJdbi(config, environment);

    environment.addResource(new ProjectResource(jdbi.onDemand(ProjectRepository.class)));
    environment.addResource(new DonorResource(jdbi.onDemand(DonorRepository.class)));
    environment.addResource(new SpecimenResource(jdbi.onDemand(SpecimenRepository.class)));
    environment.addResource(new SampleResource(jdbi.onDemand(SampleRepository.class)));
    environment.addResource(new MutationResource(jdbi.onDemand(MutationRepository.class)));

    environment.addProvider(BadRequestExceptionMapper.class);
    environment.addProvider(NotFoundExceptionMapper.class);

    ConsoleReporter.enable(30, TimeUnit.MINUTES);

    logInfo(config, args);
  }

  private static DBI createJdbi(IdentifierConfig config, Environment environment) throws ClassNotFoundException {
    DBIFactory factory = new DBIFactory();
    DBI jdbi = factory.build(environment, config.getDatabase(), "postgresql");

    return jdbi;
  }

  private static void logInfo(IdentifierConfig config, String... args) {
    log.info(repeat("=", 60));
    log.info("{} {}", APPLICATION_NAME.toUpperCase().replace('-', ' '), VersionUtils.getVersion());
    log.info(" > {}", formatArguments(args));
    log.info("SCM:");
    Properties scmInfo = VersionUtils.getScmInfo();
    for (String property : scmInfo.stringPropertyNames()) {
      String value = scmInfo.getProperty(property, "").replaceAll("\n", " ");
      log.info("         {}: {}", padEnd(property, 24, ' '), value);
    }
    log.info("Config: {}", config);
    log.info(repeat("=", 60));
  }

  private static String getJarName() {
    String jarPath = IdentifierMain.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    File jarFile = new File(jarPath);

    return jarFile.getName();
  }

  private static String formatArguments(String... args) {
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    List<String> inputArguments = runtime.getInputArguments();

    return format("java %s -jar %s %s", join(inputArguments, ' '), getJarName(), join(args, ' '));
  }
}
