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
package org.icgc.dcc.etl.loader.flow.planner;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.model.ClinicalType.CLINICAL_CORE_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.withRawSequenceData;

import static org.icgc.dcc.etl.loader.flow.planner.PipeNames.getRawSequenceDataInfoPipeName;

import java.util.Set;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FileTypes.FileSubType;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.etl.loader.cascading.RawSequenceDataInfo;
import org.icgc.dcc.etl.loader.cascading.StainedSectionImageUpdate;
import org.icgc.dcc.etl.loader.core.LoaderContext;
import org.icgc.dcc.etl.loader.core.LoaderPlan;
import org.icgc.dcc.etl.loader.flow.SummaryCollector;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategy;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;

@Slf4j
public class DonorRecordLoaderFlowPlanner extends BaseRecordLoaderFlowPlanner {

  /**
   * List of {@link FeatureType}s available for a given submission (all types are not always provided, though there
   * typically is always at least one).
   */
  private final Set<FeatureType> availableFeatureTypes;

  public DonorRecordLoaderFlowPlanner(
      @NonNull final LoaderContext context,
      @NonNull final LoaderPlan plan,
      @NonNull final String submission,
      @NonNull final Set<FeatureType> availableFeatureTypes,
      @NonNull final Set<FileSubType> availableSubTypes,
      @NonNull final LoaderPlatformStrategy platformStrategy) {
    super(context, CLINICAL_CORE_TYPE, submission, availableSubTypes, platformStrategy);
    this.availableFeatureTypes = checkNotNull(availableFeatureTypes);

    // There should always be the core clinical files at the minimum
    checkState(!availableFeatureTypes.isEmpty(), "No feature types found for submission: '%s'", submission);
  }

  SummaryCollector summary;

  @Override
  protected Pipe process(@NonNull Pipe clinicalPipe) {
    summary = new SummaryCollector(clinicalPipe, availableFeatureTypes, getSubmission());

    return summary;
  }

  @Override
  protected Pipe levelSpecificProcessing(
      @NonNull Pipe clinicalPipe,
      @NonNull final FileType currentFileType) {

    // Applicable for all clinical levels
    clinicalPipe = addProjectId(clinicalPipe, getSubmission());

    switch (currentFileType) { // TODO: consider further sub-typing instead?

    case DONOR_TYPE:
      // Nothing more to do here
      return clinicalPipe;

    case SPECIMEN_TYPE:
      return new StainedSectionImageUpdate(clinicalPipe);

    case SAMPLE_TYPE:
      return new RawSequenceDataInfo(clinicalPipe,
          getSubmission(),
          availableFeatureTypes,
          RawSequenceDataInfo.filterRawSequencingMappings(
              context.getSubmissionModel(),
              availableFeatureTypes));

    default:
      throw new IllegalStateException(String.format("Unexpected file type: '%s'", currentFileType));
    }

  }

  @Override
  protected void onConnectSources(FlowDef flowDef, LoaderPlatformStrategy platformStrategy) {
    log.info("Connecting extra sources for the {} flow ({} additional sources)",
        getDescription(), availableFeatureTypes.size());

    summary.connect(flowDef, platformStrategy);

    // Plugging taps to read meta files (raw sequence data info)
    for (val featureType : withRawSequenceData(availableFeatureTypes)) {

      val metaFileType = featureType.getMetaFileType();
      flowDef.addSource(
          getRawSequenceDataInfoPipeName(getIdentifiableProjectKey(), featureType),
          platformStrategy.getSourceTap(getSubmission(), metaFileType));
    }
  }

  @Override
  protected void onConnectSinks(FlowDef flowDef, LoaderPlatformStrategy platformStrategy) {
  }

}
