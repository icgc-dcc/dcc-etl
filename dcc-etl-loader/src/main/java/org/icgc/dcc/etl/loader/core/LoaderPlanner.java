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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.icgc.dcc.common.core.model.FileTypes.FileSubType.MANDATORY_SUBTYPES;
import static org.icgc.dcc.common.core.util.Formats.formatBytes;

import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FileTypes.FileSubType;
import org.icgc.dcc.etl.loader.flow.RecordLoaderFlowPlanner;
import org.icgc.dcc.etl.loader.flow.planner.DonorRecordLoaderFlowPlanner;
import org.icgc.dcc.etl.loader.flow.planner.ObservationRecordLoaderFlowPlanner;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategy;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Plans a release load.
 */
@Slf4j
@RequiredArgsConstructor
public class LoaderPlanner {

  @NonNull
  private final LoaderContext context;
  @NonNull
  private final LoaderPlatformStrategy platformStrategy;
  private final int maxConcurrentFlows;

  /**
   * Plans a release load based on all available submissions data.
   */
  public LoaderPlan planRelease(ProvidedDataReleaseDigest dataDigest) {

    // Plan to be populated as planning goes
    LoaderPlan plan = new LoaderPlan(dataDigest, maxConcurrentFlows);
    log.info("Provided data digest: {}", plan.getDataDigest());

    // Plan submission loads
    FlowPlannerProvider provider = new FlowPlannerProvider(plan);
    for (String projectKey : dataDigest.getProjectKeys()) {
      planSubmission(plan, provider, projectKey);
    }

    // Connect cascades
    plan.connectCascades(platformStrategy);

    return plan;
  }

  /**
   * Plans a project load based on available submission data.
   */
  private void planSubmission(LoaderPlan plan, FlowPlannerProvider provider, String projectKey) {

    long submissionSize = platformStrategy.size(projectKey);
    log.info("Using submission '{}' of size '{}' bytes", projectKey, formatBytes(submissionSize));

    val submissionDataDigest = plan.getDataDigest().getDataSubmissionDigest(projectKey);
    log.info("Data digest: '{}'", submissionDataDigest);

    val featureTypes = submissionDataDigest.getFeatureTypes();
    log.info("Available feature types: '{}'", featureTypes);

    val availableClinicalSubTypes = Sets.newHashSet(MANDATORY_SUBTYPES);

    if (submissionDataDigest.hasSupplementalType()) {
      val supplementalTypes = submissionDataDigest.getSupplementalTypes();
      log.info("Available supplemental types: '{}'", supplementalTypes);

      val allSupplementalSubTypes = new ImmutableSet.Builder<FileSubType>();

      for (val type : supplementalTypes) {
        allSupplementalSubTypes.addAll(submissionDataDigest.getSubTypes(type));
      }
      availableClinicalSubTypes.addAll(allSupplementalSubTypes.build());
    }

    // Include flow planner for donors (always)
    plan.includeFlowPlanner(projectKey,
        provider.getDonorFlowPlanner(
            projectKey, featureTypes,
            availableClinicalSubTypes, platformStrategy));

    // Include flow planners for the feature types available
    for (val featureType : featureTypes) {
      val observationFlowPlanner =
          provider.getObservationFlowPlanner(
              projectKey,
              featureType,
              submissionDataDigest
                  .getSubTypes(featureType),
              platformStrategy);

      plan.includeFlowPlanner(projectKey, observationFlowPlanner);
    }
  }

  /**
   * Provides flow planners.
   */
  private final class FlowPlannerProvider {

    private final LoaderPlan plan;

    private FlowPlannerProvider(LoaderPlan plan) {
      this.plan = checkNotNull(plan);
    }

    private DonorRecordLoaderFlowPlanner getDonorFlowPlanner(String submission,
        Set<FeatureType> availableFeatureTypes, Set<FileSubType> availableSubTypes,
        LoaderPlatformStrategy platformStrategy) {

      return new DonorRecordLoaderFlowPlanner(
          context, plan,
          submission, availableFeatureTypes, availableSubTypes, platformStrategy);
    }

    private RecordLoaderFlowPlanner getObservationFlowPlanner(
        String submission, FeatureType featureType, Set<FileSubType> availableSubTypes,
        LoaderPlatformStrategy platformStrategy) {

      return new ObservationRecordLoaderFlowPlanner(
          context, plan, featureType,
          submission, availableSubTypes, platformStrategy);
    }

  }

}
