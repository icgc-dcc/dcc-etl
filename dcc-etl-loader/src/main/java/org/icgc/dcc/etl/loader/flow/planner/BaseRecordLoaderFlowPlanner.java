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

import static cascading.flow.FlowDef.flowDef;
import static cascading.tuple.Fields.ALL;
import static cascading.tuple.Fields.RESULTS;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.String.format;
import static lombok.AccessLevel.PROTECTED;
import static org.icgc.dcc.common.cascading.CascadingOptionals.ABSENT_PIPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.common.core.util.Optionals.ABSENT_FILE_TYPE;
import static org.icgc.dcc.common.core.util.Optionals.ABSENT_STRING;
import static org.icgc.dcc.etl.loader.core.LoaderOptionals.optional;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.generatedFields;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;
import static org.icgc.dcc.etl.loader.flow.planner.PipeNames.getEndPipeName;
import static org.icgc.dcc.etl.loader.flow.planner.PipeNames.getStartPipeName;
import static org.icgc.dcc.etl.loader.identification.Identifier.requiresIdentification;
import static org.icgc.dcc.etl.loader.service.LoaderModel.Consequence.getConsequenceArrayInternalName;

import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.cascading.CascadingFunctions.CloneField;
import org.icgc.dcc.common.cascading.Flows;
import org.icgc.dcc.common.core.model.DataType;
import org.icgc.dcc.common.core.model.DataType.DataTypes;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FileTypes.FileSubType;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.model.Identifiable;
import org.icgc.dcc.common.core.model.Identifiable.Identifiables;
import org.icgc.dcc.common.core.model.SubmissionModel;
import org.icgc.dcc.etl.loader.cascading.AddEmptyArray;
import org.icgc.dcc.etl.loader.cascading.ControlledRowsRedacting;
import org.icgc.dcc.etl.loader.cascading.HandlerFunction;
import org.icgc.dcc.etl.loader.core.LoaderContext;
import org.icgc.dcc.etl.loader.core.LoaderTailType;
import org.icgc.dcc.etl.loader.flow.PipeJoin;
import org.icgc.dcc.etl.loader.flow.PreProcessor;
import org.icgc.dcc.etl.loader.flow.RecordLoaderFlowPlanner;
import org.icgc.dcc.etl.loader.identification.Identifier;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategy;
import org.icgc.dcc.etl.loader.service.LoaderModel.Persistence;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

import com.google.common.base.Optional;

@Slf4j
@RequiredArgsConstructor(access = PROTECTED)
public abstract class BaseRecordLoaderFlowPlanner implements RecordLoaderFlowPlanner {

  @NonNull
  protected final LoaderContext context;

  @NonNull
  @Getter
  // @Override
  private final DataType dataType;

  protected final Map<FileType, Pipe> heads = newHashMap();

  @NonNull
  @Getter
  protected final String submission;

  /**
   * Set of {@link FileSubType}s that are available for the current {@link FeatureType}.
   * <p>
   * For instance, {@link FeatureType#SSM_TYPE} may only have {@link FileSubType#META_SUBTYPE} and
   * {@link FileSubType#PRIMARY_SUBTYPE}, but not {@link FileSubType#SECONDARY_SUBTYPE} (this is VALID for a
   * submission).
   */
  protected final Set<FileSubType> availableSubTypes;

  /**
   * Only because of DCC-996.
   */
  @NonNull
  protected final LoaderPlatformStrategy platformStrategy;

  /**
   * Tail for the export to the file system (for all).
   */
  private Pipe mainTail;

  /**
   * Tail for the types to be exported to the mongodb (for "ssm" for instance).
   */
  @Getter
  private Optional<Pipe> optionalTail = ABSENT_PIPE; // Default is absent

  private SubmissionModel submissionModel;

  @Override
  public String getDescription() {
    return format("%s-%s", getSubmission(), dataType);
  }

  @Override
  public void plan() {
    submissionModel = context.getSubmissionModel();
    Pipe pipe = startPlan();
    pipe = process(pipe);
    finishPlan(pipe);
  }

  /**
   * Recursively goes through the schema and plan the first part of the flow for each types.
   */
  protected Pipe startPlan() {
    checkState(availableSubTypes != null && !availableSubTypes.isEmpty(),
        "Expecting at least one sub type to be provided: %s", availableSubTypes);

    log.info("Starting recursive browsing of schema {}", dataType);
    {
      val topLevelFileType = getDataType().getTopLevelFileType();
      checkState(
          hasAvailableData(topLevelFileType),
          "Matching file for top-level file schema is always expected to be provided, nothing found for '%s' in '%s' ('%s')",
          topLevelFileType, getDescription(), availableSubTypes);
      int rootDepth = 0;
      return assemble(topLevelFileType, rootDepth);
    }
  }

  /**
   * Recursively transforms a {@code RecordSchema} into to a flow assembly represented by a {@code Pipe}.
   * <p>
   * TODO: make use of {@link CodesTranslator} and {@link NullConverter} instead.
   */
  protected Pipe assemble(FileType currentFileType, int depth) {

    log.info("Assembling '{}': '{}'",
        getDescription(), currentFileType);

    Pipe pipe = new Pipe(getStartPipeName(getIdentifiableProjectKey(), currentFileType));
    heads.put(currentFileType, pipe);
    pipe = preProcess(pipe, currentFileType);

    if (hasMetaFileType(currentFileType)) {
      pipe = assembleMeta(pipe, currentFileType, getMetaFileType().get());
    }

    if (requiresIdentification(currentFileType)) {
      pipe = identify(pipe, currentFileType);
    }

    for (val nestedFileType : Persistence.getNestedTypes(currentFileType)) {
      pipe = hasAvailableData(nestedFileType) ?
          assembleArray(
              pipe, depth,
              currentFileType, nestedFileType) :

          // Adds an empty array to the tuple stream
          new AddEmptyArray(pipe, getConsequenceArrayInternalName(nestedFileType));
    }

    return levelSpecificProcessing(pipe, currentFileType);
  }

  /**
   * Allows subclass to add processing for a given level (for instance adding the "available_raw_sequence_data" info at
   * the sample level).
   */
  protected abstract Pipe levelSpecificProcessing(Pipe pipe, FileType currentFileType);

  private Pipe assembleMeta(Pipe pipe, FileType mainFileType, FileType metaFileType) {
    log.info("Assembling meta file: '{}'", metaFileType);

    Pipe metaPipe = new Pipe(getStartPipeName(getIdentifiableProjectKey(), metaFileType));
    heads.put(metaFileType, metaPipe);

    metaPipe = preProcess(metaPipe, metaFileType);

    return getPipeJoin(
        mainFileType, pipe,
        metaFileType, metaPipe); // No need to provide an array name (no nesting)
  }

  private Pipe assembleArray(
      Pipe pipe, int depth,
      FileType parentFileType, FileType arrayFileType) {
    log.info("Processing non-meta nested file schema '{}' since data is available for it in {}",
        arrayFileType, getDescription());

    return getPipeJoin(
        arrayFileType,

        // Recurse into record and joining as required
        assemble(arrayFileType, depth + 1), // The bigger one (for instance ssm_s or sample)

        parentFileType, pipe, // The smaller one (for instance ssm_p or specimen)
        Optional.of(getConsequenceArrayInternalName(arrayFileType))); // Provide array name for nesting
  }

  protected Pipe preProcess(
      @NonNull final Pipe pipe,
      @NonNull final FileType currentFileType) {
    return new PreProcessor(
        pipe,
        currentFileType,
        submissionModel.getFieldNames(currentFileType),
        platformStrategy.getFileHeader(getSubmission(), currentFileType),
        submissionModel.getValueTypes(currentFileType),
        submissionModel.getFileCodeList(currentFileType));
  }

  private Pipe getPipeJoin(
      @NonNull final FileType referencingFileType,
      @NonNull final Pipe referencingPipe,
      @NonNull final FileType referencedFileType,
      @NonNull final Pipe referencedPipe) {

    return getPipeJoin(
        referencingFileType, referencingPipe,
        referencedFileType, referencedPipe,
        ABSENT_STRING);
  }

  private Pipe getPipeJoin(
      @NonNull final FileType referencingFileType,
      @NonNull final Pipe referencingPipe,
      @NonNull final FileType referencedFileType,
      @NonNull final Pipe referencedPipe,
      @NonNull final Optional<String> nestingFieldName) {

    checkConsistency(referencedFileType, submissionModel.getReferencedFileType(referencingFileType));

    return new PipeJoin(
        referencingFileType, referencingPipe, submissionModel.getFks(referencingFileType),
        referencedFileType, referencedPipe, submissionModel.getPks(referencedFileType),
        submissionModel.isInnerJoin(referencingFileType),
        nestingFieldName);
  }

  protected static Pipe addProjectId(
      @NonNull final Pipe clinicalPipe,
      @NonNull final String projectKey) {
    return new Each(
        clinicalPipe,
        new Insert(
            generatedFields(PROJECT_ID),
            projectKey),
        ALL);
  }

  protected static Pipe cloneField(
      @NonNull final Pipe pipe,
      @NonNull final FileType fileType,
      @NonNull final String originalFieldName,
      @NonNull final String newFieldName) {
    return new Each(
        pipe,
        new CloneField(
            prefixedFields(fileType, originalFieldName),
            generatedFields(newFieldName)),
        ALL);
  }

  /**
   * Processes the data is a way that is specific to the type of data (clinical versus experimental).
   */
  protected abstract Pipe process(Pipe pipe);

  /**
   * Finishes the planning for the schema and preparing the tail(s).
   */
  protected void finishPlan(Pipe currentTail) {
    if (context.isFilterAllControlled() && isSsm()) {
      log.info("Filtering out ALL controlled rows");

      currentTail = new ControlledRowsRedacting(currentTail);
    }

    // Set tail export to filesystem
    String fileSystemPipeName = getEndPipeName(getIdentifiableProjectKey(), LoaderTailType.FS);

    log.info("Creating file system pipe {}", fileSystemPipeName);
    mainTail = new Pipe(fileSystemPipeName, currentTail);

    // Transforming nested tuples into JSON documents strings
    log.info("Adding handler pipe for {}", dataType);

    mainTail = new Each(mainTail, ALL, new HandlerFunction(submissionModel, getDataType()), RESULTS);

    // Split flow if type is to be stored in mongodb as well
    if (isMongoSinkable()) {
      String mongoPipeName = getEndPipeName(getIdentifiableProjectKey(), LoaderTailType.MONGODB);
      log.info("Creating mongo pipe {}", mongoPipeName);
      Pipe alternativeTail = new Pipe(mongoPipeName, currentTail);

      if (!context.isFilterAllControlled() && isSsm()) {
        log.info("Filtering out controlled rows for portal only");

        alternativeTail = new ControlledRowsRedacting(alternativeTail);
      }

      optionalTail = optional(alternativeTail);
    }
  }

  @Override
  public Flow<?> connect(LoaderPlatformStrategy platformStrategy) {
    log.info("Connecting {}", getDescription());

    val flowDef = flowDef().setName(getFlowName());

    // Add taps
    addSourceTaps(flowDef, platformStrategy);
    addSinkTaps(flowDef, platformStrategy);

    // Notify subclasses
    onConnectSources(flowDef, platformStrategy);
    onConnectSinks(flowDef, platformStrategy);

    return connectFlow(flowDef, platformStrategy);
  }

  /**
   * Template method to allow subclasses a hook to alter sources connection behaviour.
   * <p>
   * Make sure to account for the FS versus MongoDB destination split if changing sinks (DCC-789 especially).
   */
  protected abstract void onConnectSources(FlowDef flowDef, LoaderPlatformStrategy platformStrategy);

  /**
   * Template method to allow subclasses a hook to alter sinks connection behaviour.
   * <p>
   * Make sure to account for the FS versus MongoDB destination split if changing sinks (DCC-789 especially).
   */
  protected abstract void onConnectSinks(FlowDef flowDef, LoaderPlatformStrategy platformStrategy);

  /**
   * Adds the platform source taps to {@code flowDef} subject to the supplied {@code platformStrategy}.
   */
  private void addSourceTaps(FlowDef flowDef, final LoaderPlatformStrategy platformStrategy) {
    log.info("Adding source taps for {}", dataType);

    // From schemaName -> Pipe mapping create a schemaName -> Tap mapping
    for (val entry : heads.entrySet()) {
      val fileType = entry.getKey();
      Pipe pipe = entry.getValue();

      // Identify a role-specific tap implementation based on the current execution platform
      val sourceTap = platformStrategy.getSourceTap(getSubmission(), fileType);
      log.info("Adding source tap for '{}'", fileType);

      flowDef.addSource(pipe, sourceTap);
    }
  }

  /**
   * Adds the platform sink taps to {@code flowDef} subject to the supplied {@code platformStrategy}.
   */
  private void addSinkTaps(FlowDef flowDef, final LoaderPlatformStrategy platformStrategy) {
    log.info("Adding sink taps for {}", dataType);

    // Add file system taps
    val fileSystemTap = platformStrategy.getFileSystemTap(getDataType(), getSubmission());
    log.info("Adding file system sink to {}, using tap {}", mainTail.getName(), fileSystemTap.getIdentifier());
    flowDef.addTailSink(mainTail, fileSystemTap);

    // Add mongodb tap when applicable (only for the types stored in mongo)
    if (optionalTail.isPresent()) {
      Pipe mongoTail = optionalTail.get();

      checkState(isMongoSinkable(), // By design but just to be safe
          "{} is not meant to be loaded in mongo", dataType);
      val databaseTap = platformStrategy.getDatabaseTap(getDataType());
      log.info("Adding mongo sink to {}, using tap {}",
          mongoTail.getName(), databaseTap.getIdentifier());

      flowDef.addTailSink(mongoTail, databaseTap);
    } else {
      log.info("NOT adding {} counterpart sink to '{}'",
          LoaderTailType.MONGODB, mainTail.getName());
    }
  }

  /**
   * Performs the platform based connection of the flow.
   */
  private Flow<?> connectFlow(FlowDef flowDef, LoaderPlatformStrategy platformStrategy) {
    return platformStrategy
        .getFlowConnector()
        .connect(flowDef);
  }

  private Pipe identify(Pipe pipe, FileType currentFileType) {

    return Identifier.identify(
        context.getIdentifierClientClassName(),
        context.getIdentifierUri(),
        context.getReleaseName(),
        pipe,
        getSubmission(),
        currentFileType,
        hasMetaFileType(currentFileType) ?
            getMetaFileType() :
            ABSENT_FILE_TYPE);
  }

  /**
   * Determines whether an observation file schema requires import of a surrogate matched_sample_id (only if it contains
   * a matched_sample_id basically).
   */
  protected boolean requiresMatchedSampleIdImport(FeatureType featureType) {

    return submissionModel.getFieldNames(featureType.getMetaFileType())
        .contains(SUBMISSION_MATCHED_SAMPLE_ID);
  }

  private static void checkConsistency(FileType referencedFileType, FileType referencedFileTypeTmp) {
    checkState(referencedFileTypeTmp == referencedFileType,
        "Inconsistent transform files: '%s' != '%s'",
        referencedFileTypeTmp, referencedFileType);
  }

  private Optional<FileType> getMetaFileType() {
    return getDataType().isClinicalType() ?
        ABSENT_FILE_TYPE :
        Optional.of(getDataType().asFeatureType().getMetaFileType());
  }

  protected Identifiable getIdentifiableProjectKey() {
    return Identifiables.fromString(getSubmission());
  }

  protected boolean isSsm() {
    return SSM_TYPE == getDataType();
  }

  /**
   * Determines if there is data available for the file schema under consideration. For instance there could be data for
   * ssm_m and ssm_p but not for ssm_s.
   */
  private boolean hasAvailableData(@NonNull FileType fileType) {
    return availableSubTypes.contains(fileType.getSubType());
  }

  private boolean hasMetaFileType(@NonNull final FileType fileType) {
    return dataType.isFeatureType() && fileType.isPrimary();
  }

  private boolean isMongoSinkable() {
    return DataTypes.isMongoSinkable(getDataType());
  }

  private String getFlowName() {
    return Flows.getNameFromIdentifiables(
        Identifiables.fromString(getSubmission()),
        getDataType());
  }

}
