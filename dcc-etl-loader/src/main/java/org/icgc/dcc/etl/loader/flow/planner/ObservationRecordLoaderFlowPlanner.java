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
package org.icgc.dcc.etl.loader.flow.planner;

import static cascading.tuple.Fields.ALL;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_GENE_AFFECTED;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_TRANSCRIPT_AFFECTED;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SAMPLE_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SPECIMEN_TYPE;
import static org.icgc.dcc.etl.loader.flow.ClinicalHack.observationClinicalLeftJoinFields;
import static org.icgc.dcc.etl.loader.flow.ClinicalHack.observationClinicalObservationControlRightJoinFields;
import static org.icgc.dcc.etl.loader.flow.ClinicalHack.observationClinicalTumourRightJoinFields;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.generatedFields;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;
import static org.icgc.dcc.etl.loader.flow.planner.PipeNames.getClinicalHackPipeName;

import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames;
import org.icgc.dcc.common.core.model.FileTypes.FileSubType;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.etl.loader.core.LoaderContext;
import org.icgc.dcc.etl.loader.core.LoaderPlan;
import org.icgc.dcc.etl.loader.flow.ClinicalHack;
import org.icgc.dcc.etl.loader.flow.ObservationNesting;
import org.icgc.dcc.etl.loader.flow.SummaryCollector;
import org.icgc.dcc.etl.loader.flow.SummaryPreComputation;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategy;
import org.icgc.dcc.etl.loader.service.LoaderModel;

import cascading.flow.FlowDef;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.LeftJoin;

@Slf4j
public class ObservationRecordLoaderFlowPlanner extends BaseRecordLoaderFlowPlanner {

  /**
   * Pipe used for communicating summary information to the {@link DonorRecordLoaderFlowPlanner}, which will combine
   * such pipes for all {@link ObservationRecordLoaderFlowPlanner} in a given submission.
   */
  private Pipe summaryPipe;

  /**
   * Because of cascading bug: DCC-753
   */
  private Pipe specimenPipe;
  private Pipe samplePipe;

  public ObservationRecordLoaderFlowPlanner(
      @NonNull final LoaderContext context,
      @NonNull final LoaderPlan plan,
      @NonNull final FeatureType featureType,
      @NonNull final String submission,
      @NonNull final Set<FileSubType> availableSubTypes,
      @NonNull final LoaderPlatformStrategy platformStrategy) {

    super(context, featureType, submission, availableSubTypes, platformStrategy);
  }

  private FeatureType getFeatureType() {
    return getDataType().asFeatureType();
  }

  @Override
  protected Pipe process(Pipe observationPipe) {

    observationPipe = joinClinicalData(observationPipe, new ScopeLimiter().reproduceClinicalFlow());

    if (isSsm()) { // TODO: subclass for SSM?
      observationPipe = new ObservationNesting(observationPipe);
    }

    observationPipe = addProjectId(observationPipe, getSubmission());

    observationPipe = addObservationType(
        observationPipe,
        getDataType().getId());

    // Create summary branch for consumption by the clinical flow
    summaryPipe = new SummaryPreComputation(
        observationPipe,
        getSubmission(),
        getFeatureType(),
        PipeNames.getSummaryPipeName(getIdentifiableProjectKey(), getFeatureType()));

    return observationPipe;
  }

  @Override
  protected Pipe levelSpecificProcessing(
      @NonNull Pipe observationPipe,
      @NonNull final FileType currentFileType) {

    if (requiresAnnotationInfoCloning(currentFileType)) { // TODO: subclass rather?
      // Clone gene ID
      observationPipe = cloneField(
          observationPipe, currentFileType,
          SUBMISSION_GENE_AFFECTED, LoaderFieldNames.GENE_ID);

      // Clone transcript ID
      observationPipe = cloneField(
          observationPipe, currentFileType,
          SUBMISSION_TRANSCRIPT_AFFECTED, LoaderFieldNames.TRANSCRIPT_ID);
    }

    return observationPipe;
  }

  private static Pipe addObservationType(
      @NonNull final Pipe observationPipe,
      @NonNull final String dataTypeId) {
    return new Each(
        observationPipe,
        new Insert(
            generatedFields(OBSERVATION_TYPE),
            dataTypeId),
        ALL);
  }

  /**
   * Join the reproduced clinical joins with the observation (to import clinical IDs).
   */
  private Pipe joinClinicalData(
      @NonNull Pipe observationPipe,
      @NonNull Pipe clinicalPipe) {

    val featureType = getDataType().asFeatureType();
    val primaryFileType = featureType.getPrimaryFileType();

    // Join to import the tumour sample ID into observations
    observationPipe = new HashJoin(
        observationPipe, observationClinicalLeftJoinFields(primaryFileType),
        clinicalPipe, observationClinicalTumourRightJoinFields(),
        new InnerJoin());

    // Discard temporary join fields to only leave the imported fields
    observationPipe = new Discard(observationPipe, observationClinicalTumourRightJoinFields());

    if (requiresMatchedSampleIdImport(featureType)) {
      log.info("Importing matched sample ID for {}", featureType);

      // Prepare import of the control sample ID (recycle clinical pipe)
      clinicalPipe = new Retain(clinicalPipe,
          observationClinicalTumourRightJoinFields()
              .append(generatedFields(DONOR_SAMPLE_ID))); // Will be more appropriately renamed in the next step

      // Rename to avoid field name conflicts
      clinicalPipe = new Rename(clinicalPipe,
          observationClinicalTumourRightJoinFields()
              .append(generatedFields(DONOR_SAMPLE_ID)),
          observationClinicalObservationControlRightJoinFields()
              .append(generatedFields(LoaderFieldNames.SURROGATE_MATCHED_SAMPLE_ID)));

      // Join to import the control sample ID into observations
      observationPipe = new HashJoin(
          observationPipe, prefixedFields(featureType.getMetaFileType(), SUBMISSION_MATCHED_SAMPLE_ID),
          clinicalPipe, observationClinicalObservationControlRightJoinFields(),
          new LeftJoin()); // Left join because control sample ID is optional

      // Discard temporary join fields to only leave the imported fields
      observationPipe = new Discard(observationPipe, observationClinicalObservationControlRightJoinFields());

    } else {
      log.info("Schema '{}' does not require importing of matched sample ID", featureType);
    }

    return observationPipe;
  }

  @Override
  protected void onConnectSources(FlowDef flowDef, LoaderPlatformStrategy platformStrategy) {
    connectReproducedClinical(flowDef, platformStrategy);
  }

  private void connectReproducedClinical(FlowDef flowDef, LoaderPlatformStrategy platformStrategy) {
    flowDef.addSource(
        specimenPipe,
        platformStrategy.getSourceTap(getSubmission(), SPECIMEN_TYPE));
    flowDef.addSource(
        samplePipe,
        platformStrategy.getSourceTap(getSubmission(), SAMPLE_TYPE));
  }

  @Override
  protected void onConnectSinks(FlowDef flowDef, LoaderPlatformStrategy platformStrategy) {
    log.info("Adding summary sink tap for {} ({})", getDescription(), getFeatureType());
    flowDef.addTailSink(
        summaryPipe,
        platformStrategy.getSummaryTap(
            getSubmission(),
            new SummaryCollector.SummaryTapInfo(getFeatureType())));
  }

  private boolean requiresAnnotationInfoCloning(@NonNull final FileType currentFileType) {
    return LoaderModel.REQUIRES_ANNOTATION_INFO_CLONING.contains(getDataType())
        && currentFileType.isSecondary();
  }

  @NoArgsConstructor(access = PRIVATE)
  private final class ScopeLimiter {

    /**
     * Prepare the clinical branch (to be joined with observation one later)
     */
    private Pipe reproduceClinicalFlow() {
      samplePipe = getClinicalHackStartPipe(SAMPLE_TYPE);
      specimenPipe = getClinicalHackStartPipe(SPECIMEN_TYPE);

      return new ClinicalHack(
          samplePipe,
          specimenPipe,
          context.getReleaseName(),
          getIdentifiableProjectKey(),
          context.getIdentifierUri(),
          context.getIdentifierClientClassName(),
          context.getIdentifierAuthToken());
    }

    private Pipe getClinicalHackStartPipe(@NonNull final FileType fileType) {
      return preProcess(
          new Pipe(
              getClinicalHackPipeName(
                  getIdentifiableProjectKey(),
                  fileType)),
          fileType);
    }

  }

}
