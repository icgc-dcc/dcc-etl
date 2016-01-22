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
package org.icgc.dcc.etl.annotator.service;

import static cascading.cascade.CascadeDef.cascadeDef;
import static cascading.flow.FlowDef.flowDef;

import java.util.Collection;
import java.util.List;

import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.etl.annotator.cascading.ProjectAnnotationSubAssembly;
import org.icgc.dcc.etl.annotator.cascading.TapFactory;
import org.icgc.dcc.etl.annotator.config.SnpEffProperties;
import org.icgc.dcc.etl.annotator.model.AnnotatedFileType;
import org.icgc.dcc.etl.annotator.model.AnnotatorJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.CascadeDef;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.tap.Tap;

import com.google.common.collect.ImmutableList;

/**
 * Service that annotates IGCG SGV / SSM primary files to create ICGC secondary files.
 */
@Slf4j
@Setter
@Service
public class AnnotatorService {

  /**
   * Dependencies.
   */
  @Autowired
  private SnpEffProperties properties;
  @Autowired
  private TapFactory tapFactory;
  @Autowired
  private FlowConnector flowConnector;
  @Autowired
  private AnnotatorJobFactory jobFactory;

  @SneakyThrows
  public void annotate(@NonNull String workingDir, Iterable<String> projectNames, Iterable<AnnotatedFileType> fileTypes) {
    val job = jobFactory.createAnnotatorJob(workingDir, projectNames, fileTypes);
    if (job.isEmpty()) {
      log.warn("No files found to annotate");
      return;
    }

    log.info("Creating cascade...");
    val cascade = createCascade(job);
    log.info("Finished creating cascade.");

    log.info("Synchronously completing cascade...");
    cascade.complete();
    log.info("Finished synchronously completing cascade.");
  }

  private Cascade createCascade(AnnotatorJob job) {
    val cascadeDef = cascadeDef().setName("annotator-cascade");

    for (val projectName : job.getProjectNames()) {
      addFlows(cascadeDef, createProjectFlows(projectName, job));
    }

    return new CascadeConnector().connect(cascadeDef);
  }

  private static void addFlows(CascadeDef cascadeDef, Collection<Flow<?>> flows) {
    for (val flow : flows) {
      log.info("Adding flow: {}", flow);
      cascadeDef.addFlow(flow);
    }
  }

  private List<Flow<?>> createProjectFlows(String projectName, AnnotatorJob job) {
    log.info("Processing project : {}", projectName);
    val result = new ImmutableList.Builder<Flow<?>>();
    for (val inputPath : job.getFilesByProject(projectName)) {
      log.info("  Path: {} - {}", projectName, inputPath);
      val fileType = AnnotatedFileType.byPath(inputPath);
      val assembly = new ProjectAnnotationSubAssembly(projectName, fileType, properties);
      val outputPath = job.getOutputPath(projectName, fileType);
      val flowDef = flowDef()
          .setName("annotator-" + projectName + "-" + fileType.getId() + "-flow")
          .addSource(assembly, getInputTap(inputPath))
          .addTailSink(assembly, getOutputTap(outputPath));
      result.add(flowConnector.connect(flowDef));
    }

    return result.build();
  }

  private Tap<?, ?, ?> getOutputTap(Path path) {
    return tapFactory.createFileTap(path);
  }

  private Tap<?, ?, ?> getInputTap(Path path) {
    return tapFactory.createFileTap(path);
  }

}
