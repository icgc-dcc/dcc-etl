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
package org.icgc.dcc.etl.annotator.cascading;

import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_VARIANT_ALLELE;
import static org.icgc.dcc.etl.annotator.model.AnnotatedFileType.SSM;
import static org.icgc.dcc.etl.core.util.Mutations.createMutation;

import java.util.List;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.annotator.config.SnpEffProperties;
import org.icgc.dcc.etl.annotator.converter.ICGCToVCFConverter.MutationType;
import org.icgc.dcc.etl.annotator.model.AnnotatedFileType;
import org.icgc.dcc.etl.annotator.model.SecondaryEntity;
import org.icgc.dcc.etl.annotator.model.SecondaryTupleAdapter;
import org.icgc.dcc.etl.annotator.snpeff.SnpEffPredictor;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Cascading {@link Function} that maps from ICGC primary to secondary records. This relationship is one-to-many.
 */
@Slf4j
@SuppressWarnings("rawtypes")
public class SnpEffFunction extends BaseOperation<SnpEffPredictor> implements Function<SnpEffPredictor> {

  private static final String MISSING_ALLELE = "-";

  /**
   * Configuration.
   */
  private final SnpEffProperties properties;
  @NonNull
  private final AnnotatedFileType fileType;

  public SnpEffFunction(@NonNull SnpEffProperties properties, AnnotatedFileType fileType) {
    super(fileType.getFieldsNumber(), new Fields(fileType.getSecondaryFileFields()));
    this.properties = properties;
    this.fileType = fileType;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<SnpEffPredictor> call) {
    val predictor = new SnpEffPredictor(properties, fileType);

    log.info("Asynchronously forking SnpEff process...");
    predictor.start();
    log.info("Successfully asynchronously forked SnpEff process");

    call.setContext(predictor);
  }

  @Override
  @SneakyThrows
  public void cleanup(FlowProcess flowProcess, OperationCall<SnpEffPredictor> call) {
    val predictor = call.getContext();

    log.info("Shutdown the forked SnpEff process...");
    predictor.stop();
    log.info("Successfully shutdown the forked SnpEff process");
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<SnpEffPredictor> call) {
    // Shorthands
    val arguments = call.getArguments();
    val predictor = call.getContext();
    val collector = call.getOutputCollector();

    // Determine the prediction
    val predictions = predict(arguments, predictor);
    for (val prediction : predictions) {
      collector.add(SecondaryTupleAdapter.secondaryToTuple(prediction));
    }
  }

  private List<SecondaryEntity> predict(TupleEntry arguments, SnpEffPredictor predictor) {
    // Extract row values
    val chromosome = arguments.getString(fileType.getChromosomeFieldName());
    val start = arguments.getLong(fileType.getChromosomeStartFieldName());
    val end = arguments.getLong(fileType.getChromosomeEndFieldName());
    val mutation = getMutation(arguments, fileType);
    val type = MutationType.fromId(arguments.getString(fileType.getMutationTypeFieldName()));
    val ref = arguments.getString(fileType.getReferenceAlleleFieldName());
    val reference = (ref.equals(MISSING_ALLELE)) ? "" : ref;
    val id = arguments.getString(fileType.getObservationIdFieldName());

    return predictor.predict(chromosome, start, end, mutation, type, reference, id);
  }

  private static String getMutation(TupleEntry arguments, AnnotatedFileType fileType) {
    if (fileType == SSM) {
      return arguments.getString(SUBMISSION_MUTATION);
    }

    val mutatedFrom = arguments.getString(fileType.getReferenceAlleleFieldName());
    val mutatedTo = arguments.getString(SUBMISSION_OBSERVATION_VARIANT_ALLELE);

    return createMutation(mutatedFrom, mutatedTo);
  }

}
