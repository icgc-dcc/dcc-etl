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
package org.icgc.dcc.etl.importer.project;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.io.IOException;
import java.util.Set;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.client.api.ICGCClient;
import org.icgc.dcc.common.client.api.ICGCClientConfig;
import org.icgc.dcc.common.client.api.cgp.CGPClient;
import org.icgc.dcc.etl.importer.project.model.Project;
import org.icgc.dcc.etl.importer.project.reader.ProjectReader;
import org.icgc.dcc.etl.importer.project.util.ProjectFilter;
import org.icgc.dcc.etl.importer.project.writer.ProjectWriter;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.mongodb.MongoClientURI;

@Slf4j
public class ProjectImporter {

  /**
   * Configuration
   */
  private final ICGCClientConfig config;
  private final MongoClientURI mongoUri;
  private final Optional<Set<String>> projectNames;

  /**
   * Dependencies.
   */
  private final CGPClient client;

  public ProjectImporter(@NonNull ICGCClientConfig config, @NonNull MongoClientURI mongoUri,
      @NonNull Optional<Set<String>> projectNames) {
    this(config, projectNames, mongoUri);
  }

  public ProjectImporter(@NonNull ICGCClientConfig config, @NonNull Optional<Set<String>> projectNames,
      @NonNull MongoClientURI mongoUri) {
    this.config = config;
    this.projectNames = projectNames;
    this.mongoUri = mongoUri;

    this.client = ICGCClient.create(config).cgp().details();
  }

  @SneakyThrows
  public void execute() {
    val watch = Stopwatch.createStarted();

    log.info("Reading projects using {}...", config);
    val projects = readProjects();

    log.info("Filtering projects...");
    val filteredProjects = filterProjects(projects);

    log.info("Writing {} projects to {}...", formatCount(filteredProjects), mongoUri);
    writeProjects(filteredProjects);

    log.info("Finished writing {} of {} projects in {}",
        formatCount(filteredProjects), formatCount(projects), watch);
  }

  private Iterable<Project> readProjects() {
    return new ProjectReader(client).read();
  }

  private Iterable<Project> filterProjects(Iterable<Project> projects) {
    if (projectNames.isPresent()) {
      return ProjectFilter.filterProjectsIn(projects, projectNames.get());
    } else {
      return projects;
    }
  }

  private void writeProjects(Iterable<Project> specifiedProjects) throws IOException {
    @Cleanup
    val writer = new ProjectWriter(mongoUri);
    writer.write(specifiedProjects);
  }

}
