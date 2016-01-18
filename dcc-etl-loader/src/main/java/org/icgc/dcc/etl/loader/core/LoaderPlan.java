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
package org.icgc.dcc.etl.loader.core;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static java.lang.String.format;
import static org.icgc.dcc.etl.loader.core.LoaderCascadings.connectCascade;
import static org.icgc.dcc.etl.loader.core.LoaderCascadings.createCascadeDef;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.loader.flow.RecordLoaderFlowPlanner;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategy;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeDef;
import cascading.flow.Flow;
import cascading.flow.planner.PlannerException;

/**
 * Container/initializer for the {@link RecordLoaderFlowPlanner}s.
 * <p>
 * TODO: DCC-1277 (SRP) + comment
 */
@Slf4j
@RequiredArgsConstructor
public class LoaderPlan implements Serializable {

  private static final String SYSTEM_PLANNER_NAME = "__SYSTEM__";

  /**
   * Where DOT files are written for debugging.
   */
  private static final String DOT_FILE_DIR = "/tmp";

  /**
   * Map of projects to their list of flow planners (See {@link RecordLoaderFlowPlanner} for details).
   */
  private final Map<String, List<RecordLoaderFlowPlanner>> flowPlanners = newLinkedHashMap();

  /**
   * Description of the data provided for the release.
   */
  @Getter
  private final ProvidedDataReleaseDigest dataDigest;

  /**
   * Maximum number of parallel flows (defaulted but configurable).
   */
  private final int maxConcurrentFlows;

  @Getter
  private Cascade cascade;

  void includeFlowPlanner(RecordLoaderFlowPlanner recordFlowPlanner) {
    checkState(false); // Not currently used (may disappear in the future)
    includeFlowPlanner(SYSTEM_PLANNER_NAME, recordFlowPlanner);
  }

  void includeFlowPlanner(String projecKey, RecordLoaderFlowPlanner recordFlowPlanner) {
    // Keep track of the flow planner for future "connect()" step
    String className = recordFlowPlanner.getClass().getSimpleName();
    log.info("Adding {} flow planner `{}`", className, projecKey);
    addFlowPlanner(projecKey, recordFlowPlanner);

    // Actually plan the flow
    recordFlowPlanner.plan();
  }

  private void addFlowPlanner(String submission, RecordLoaderFlowPlanner recordFlowPlanner) {
    List<RecordLoaderFlowPlanner> submissionFlowPlanners = flowPlanners.get(submission);
    if (submissionFlowPlanners == null) {
      submissionFlowPlanners = newArrayList();
      flowPlanners.put(submission, submissionFlowPlanners);
    }
    submissionFlowPlanners.add(recordFlowPlanner);
  }

  void connectCascades(LoaderPlatformStrategy platformStrategy) {
    try {
      log.info("Creating cascade with '{}' max concurrent flows", maxConcurrentFlows);
      CascadeDef cascadeDef = createCascadeDef("loader")
          .setMaxConcurrentFlows(maxConcurrentFlows);

      for (val entry : flowPlanners.entrySet()) {
        String submission = entry.getKey();
        val flowPlanners = entry.getValue();

        // Connect and add the flows to cascasde
        for (val flowPlanner : flowPlanners) {
          Flow<?> flow = flowPlanner.connect(platformStrategy);

          if (flow != null) {
            log.info("Adding flow for {}: {}", submission, flowPlanner.getDataType());
            cascadeDef.addFlow(flow);
          }
        }

      }

      cascade = connectCascade(cascadeDef);
    } catch (PlannerException e) {
      e.writeDOT(dotFile("loader-cascade-exception.dot"));
      throw e;
    }
  }

  static String dotFile(String fileName, Object... args) {
    return format(DOT_FILE_DIR + "/" + fileName, args);
  }

}
