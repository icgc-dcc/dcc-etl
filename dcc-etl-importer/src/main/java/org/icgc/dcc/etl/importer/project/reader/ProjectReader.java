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
package org.icgc.dcc.etl.importer.project.reader;

import static org.icgc.dcc.etl.importer.project.util.ProjectConverter.convertCgp;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.client.api.cgp.CGPClient;
import org.icgc.dcc.common.client.api.cgp.CancerGenomeProject;
import org.icgc.dcc.etl.importer.project.model.Project;

import com.google.common.collect.ImmutableList;

@Slf4j
@RequiredArgsConstructor
public class ProjectReader {

  @NonNull
  private final CGPClient client;

  public Iterable<Project> read() {
    val projects = ImmutableList.<Project> builder();

    int projectNumber = 1;

    val cgps = readCgps();
    for (val value : cgps) {
      val cgp = readCgp(value);

      val cgpProjects = convertCgp(cgp);
      for (val cgpProject : cgpProjects) {
        log.info("[{}/{}] Read CGP project {}", projectNumber, cgps.size(), cgpProject.get__project_id());
      }

      projects.addAll(cgpProjects);
      projectNumber++;
    }

    return projects.build();
  }

  private List<CancerGenomeProject> readCgps() {
    return client.getCancerGenomeProjects();
  }

  private CancerGenomeProject readCgp(CancerGenomeProject value) {
    return client.getCancerGenomeProject(value.getNid());
  }

}
