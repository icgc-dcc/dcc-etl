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
package org.icgc.dcc.etl.loader.flow;

import static cascading.tuple.Fields.ALL;
import static cascading.tuple.Fields.ARGS;
import static cascading.tuple.Fields.REPLACE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static org.icgc.dcc.common.cascading.Fields2.getFieldNames;
import static org.icgc.dcc.common.cascading.Tuples2.isNullField;
import static org.icgc.dcc.common.cascading.Tuples2.nestTuple;
import static org.icgc.dcc.etl.loader.flow.SummaryHelper.getAvailableDataTypeField;
import static org.icgc.dcc.etl.loader.flow.SummaryHelper.getDonorIdField;
import static org.icgc.dcc.etl.loader.flow.SummaryHelper.getSummaryTempDonorIdField;
import static org.icgc.dcc.etl.loader.flow.SummaryHelper.getSummaryValueField;
import static org.icgc.dcc.etl.loader.flow.planner.PipeNames.getSummaryPipeName;

import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.cascading.HasSingleResultField;
import org.icgc.dcc.common.cascading.SubAssemblies.Nest;
import org.icgc.dcc.common.cascading.SubAssemblies.NullReplacer;
import org.icgc.dcc.common.cascading.operation.BaseFunction;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.Identifiable.Identifiables;
import org.icgc.dcc.etl.loader.flow.planner.DonorRecordLoaderFlowPlanner;
import org.icgc.dcc.etl.loader.flow.planner.ObservationRecordLoaderFlowPlanner;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategy;
import org.icgc.dcc.etl.loader.service.LoaderModel;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.google.common.base.Joiner;

@Slf4j
public class SummaryCollector extends SubAssembly implements HasSingleResultField {

  // TODO: https://jira.oicr.on.ca/browse/DCC-2426 - generate from enum
  private static Fields SUMMARY_SUB_FIELDS = new Fields(
      "_ssm_count",
      "_sgv_exists",
      "_cnsm_exists",
      "_meth_array_exists",
      "_meth_seq_exists",
      "_mirna_seq_exists",
      "_exp_array_exists",
      "_exp_seq_exists",
      "_cngv_exists",
      "_stsm_exists",
      "_stgv_exists",
      "_pexp_exists",
      "_jcn_exists",
      "_available_data_type"
      );

  private final String projectKey;
  private final Set<FeatureType> availableFeatureTypes;

  /**
   * Pipes used by all the {@link ObservationRecordLoaderFlowPlanner}s to communicate summary information to the
   * {@link DonorRecordLoaderFlowPlanner}, they are all combined here for in a given submission.
   */
  private final Map<FeatureType, Pipe> summaryPipes = newLinkedHashMap();

  public SummaryCollector(
      Pipe clinicalPipe,
      Set<FeatureType> availableFeatureTypes,
      String projectKey) {
    this.projectKey = projectKey;
    this.availableFeatureTypes = availableFeatureTypes;
    setTails(new Discard(
        new Each(
            addTypesSummary(clinicalPipe),
            SUMMARY_SUB_FIELDS,
            new Nest(this),
            ALL),
        SUMMARY_SUB_FIELDS));
  }

  @Override
  public Fields getResultField() {
    return getStaticResultField();
  }

  public static Fields getStaticResultField() {
    return LoaderModel.Summary.SUMMARY_RESULT_FIELD;
  }

  public void connect(
      FlowDef flowDef,
      LoaderPlatformStrategy platformStrategy) {
    for (val featureType : availableFeatureTypes) {
      log.info("Connecting source for flow {} and type {}", projectKey, featureType);

      flowDef.addSource(
          summaryPipes.get(featureType),
          platformStrategy.getSummaryTap(
              projectKey,
              new SummaryCollector.SummaryTapInfo(featureType)));
    }
  }

  /**
   * Adds summary information about donors for each {@link FeatureType}, provided in the submission or not.
   */
  private Pipe addTypesSummary(Pipe pipe) {

    // Add summary for each observation (feature type) provided
    for (FeatureType featureType : availableFeatureTypes) {
      pipe = addAvailableTypeSummary(pipe, featureType);
    }

    // Complete the summary for the types not provided
    for (FeatureType featureType : FeatureType.complement(availableFeatureTypes)) {
      pipe = addMissingTypeSummary(pipe, featureType);
    }

    return addAvailableDataTypesArray(pipe);
  }

  /**
   * Adds summary information about donors for each {@link FeatureType} provided in the submission.
   */
  private Pipe addAvailableTypeSummary(Pipe clinicalPipe, FeatureType featureType) {
    log.info("Adding summary information for provided type {}", featureType);

    Pipe summaryPipe = createSummaryPipe(featureType);

    // Join the clinical flow with the summary for the given feature type (they're both small)
    clinicalPipe = new HashJoin(
        clinicalPipe, getDonorIdField(),
        summaryPipe, getSummaryTempDonorIdField(),
        new LeftJoin());

    // Discard the join field on the summary side
    clinicalPipe = new Discard(clinicalPipe, getSummaryTempDonorIdField());

    // Replace the nulls from the left join with default value (0 for counts/false for booleans)
    clinicalPipe = new Each(
        clinicalPipe,
        getSummaryValueField(featureType),
        new ReplaceNulls( // TODO: investigate if using RegexReplace would be possible (how to match
                          // java null though?)
            featureType.isCountSummary()),
        REPLACE);

    // Add pipe to map for later connection
    summaryPipes.put(featureType, summaryPipe);

    return clinicalPipe;
  }

  /**
   * Adds summary information about donors for each {@link FeatureType} <b>NOT</b> provided in the submission.
   */
  private Pipe addMissingTypeSummary(Pipe pipe, FeatureType featureType) {
    log.info("Adding summary information for missing type {} in {}", featureType, projectKey);

    return new Each(
        pipe,
        new Insert(
            getSummaryValueField(featureType),
            ReplaceNulls.getReplacementValue(featureType.isCountSummary())),
        ALL);
  }

  /**
   * Adds an array summarizing the data types available for the submission.
   */
  private Pipe addAvailableDataTypesArray(Pipe pipe) {
    log.info("Adding available data types information for {}", projectKey);

    return new Each(
        pipe,
        new AvailableDataTypes(
            getAvailableDataTypeField()),
        ALL);
  }

  private Pipe createSummaryPipe(@NonNull final FeatureType featureType) {
    return new Pipe(getSummaryPipeName(
        Identifiables.fromString(projectKey),
        featureType));
  }

  /**
   * Replaces the nulls resulting from a left join for summary data with the appropriate value (0 or false) based on the
   * feature type.
   * <p>
   * TODO: try and re-use {@link NullReplacer} (if possible at all)?
   */
  private static class ReplaceNulls extends BaseFunction<Void> {

    private static final int SUMMARY_FIELD_INDEX = 0;

    /**
     * Default value when there was no match with the left join, or when the data type was not even provided.
     */
    private static final int COUNT_DEFAULT_VALUE = 0;

    /**
     * See {@link COUNT_DEFAULT_VALUE}.
     */
    private static final boolean EXISTENCE_DEFAULT_VALUE = false;

    private final boolean countSummary;

    private ReplaceNulls(final boolean countSummary) {
      super(ARGS);
      this.countSummary = countSummary;
    }

    @Override
    public void operate(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        FunctionCall<Void> functionCall) {
      TupleEntry entry = functionCall.getArguments();

      Tuple tupleCopy = entry.getTupleCopy();
      if (isNullField(tupleCopy, SUMMARY_FIELD_INDEX)) {
        tupleCopy.set(
            SUMMARY_FIELD_INDEX,
            getReplacementValue(countSummary));
      }

      functionCall
          .getOutputCollector()
          .add(tupleCopy);
    }

    public static Object getReplacementValue(final boolean countSummary) {
      return countSummary ?
          COUNT_DEFAULT_VALUE :
          EXISTENCE_DEFAULT_VALUE;
    }

  }

  /**
   * Adds a {@link Tuple} containing the list of available data types for the submission.
   */
  private static class AvailableDataTypes extends BaseFunction<Void> {

    public AvailableDataTypes(Fields availableDataTypeField) {
      super(availableDataTypeField);
    }

    @Override
    public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess, FunctionCall<Void> functionCall) {
      TupleEntry entry = functionCall.getArguments();

      Tuple newTuple = nestTuple(createAvailableDataTypes(entry));

      functionCall
          .getOutputCollector()
          .add(newTuple);
    }

    /**
     * Creates the {@link Tuple} described at {@link AvailableDataTypes}.
     * <p>
     * TODO: move to Summary class?
     */
    private Tuple createAvailableDataTypes(TupleEntry entry) {
      Tuple availableDataTypes = new Tuple();
      for (FeatureType type : FeatureType.values()) {
        String summaryFieldName = type.getSummaryFieldName();
        if (hasPositiveCount(entry, type, summaryFieldName) ||
            isMarkedPresent(entry, type, summaryFieldName)) {
          availableDataTypes.add(type.getId());
        }
      }

      return availableDataTypes;
    }

    /**
     * Checks whether the summary field (expected to be a count) is greater than 0 or not.
     */
    private boolean hasPositiveCount(TupleEntry entry, FeatureType type, String summaryFieldName) {
      return type.isCountSummary() &&
          checkNotNull(entry.getLong(summaryFieldName), "Expecting a long for field %s", summaryFieldName) > 0;
    }

    /**
     * Checks whether the summary field (expected to be a boolean) is true (data type present) or not (data type
     * absent).
     */
    private boolean isMarkedPresent(TupleEntry entry, FeatureType type, String summaryFieldName) {
      return !type.isCountSummary()
          && checkNotNull(entry.getBoolean(summaryFieldName), "Expecting a boolean for field %s", summaryFieldName);
    }
  }

  /**
   * Holds information relevant to the intermediate output for summarizing.
   */
  public static class SummaryTapInfo {

    private static final Joiner FILENAME_JOINER = Joiner.on("-");
    private static final String FILENAME_SUFFIX = "-summary";

    @Getter
    private final Fields summaryFields;

    public SummaryTapInfo(FeatureType featureType) {
      this.summaryFields =
          SummaryHelper.getSummaryTempDonorIdField()
              .append(SummaryHelper.getSummaryValueField(
                  checkNotNull(featureType, "Expecting a non-null featureType: {}", featureType)));
    }

    /**
     * Returns the name of the intermediate file that allows {@link Flow}s to communicate summary information.
     */
    public String getFileName() {
      return FILENAME_JOINER.join(getFieldNames(summaryFields)) + FILENAME_SUFFIX;
    }
  }

}
