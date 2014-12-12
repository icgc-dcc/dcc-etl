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
package org.icgc.dcc.etl.loader.identification;

import static cascading.tuple.Fields.ALL;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ASSEMBLY_VERSION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_END;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_START;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.DONOR_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SAMPLE_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SPECIMEN_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SSM_P_TYPE;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.generatedFields;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;

import java.lang.reflect.Constructor;
import java.util.List;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.etl.loader.flow.RecordLoaderFlowPlanner;
import org.icgc.dcc.etl.loader.util.HttpIdentifierClient;
import org.icgc.dcc.etl.loader.util.IdentifierClient;
import org.icgc.dcc.common.hadoop.cascading.operation.BaseFunction;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

/**
 * Allows the {@link RecordLoaderFlowPlanner}s to identify entities.
 */
@NoArgsConstructor(access = PRIVATE)
public final class Identifier {

  /**
   * The set of file schemata for which identification must take place.
   */
  private static final List<FileType> REQUIRING_IDENTIFICATION = ImmutableList.<FileType> of(
      SSM_P_TYPE, // Because ssm_p drives the ssm_p/ssm_m combination (meta being handled in a specific matter)
      DONOR_TYPE, SPECIMEN_TYPE, SAMPLE_TYPE);

  public static Pipe identify(String identifierClientClassName, String serviceUri, String releaseName, Pipe pipe,
      String submission, FileType fileType, Optional<FileType> metaFileType) {

    return identify(
        identifierClientClassName, serviceUri, releaseName, pipe, submission,
        fileType, metaFileType,
        IdResolver.fromFileSchemaName(fileType));
  }

  /**
   * Only for the clinical hack (DCC-753), remove once addressed.
   */
  public static Pipe identify(String identifierClientClassName, String serviceUri, String releaseName, Pipe pipe,
      String submission, FileType fileType, Optional<FileType> metaFileType,
      IdResolver idResolver) {

    return new Each(
        pipe,
        ALL,
        new Identify<String>(identifierClientClassName, serviceUri, releaseName, idResolver, submission,
            fileType,
            metaFileType.isPresent() ?
                metaFileType.get() :
                null),
        ALL);
  }

  /**
   * Determines whether a file schema requires identification or not (for instance "specimen" does, but "ssm_s" does
   * not).
   */
  public static boolean requiresIdentification(FileType type) {
    return REQUIRING_IDENTIFICATION.contains(type);
  }

  /**
   * Queries the identification service in order to add ID to the tuples.
   */
  @Slf4j
  private static class Identify<T> extends BaseFunction<T> {

    private final String serviceUri;
    private final String releaseName;

    private final String identifierClientClassName;

    /**
     * The helper for resolving IDs, see {@link IdResolver}.
     */
    private final IdResolver idResolver;

    private final String projectId;

    private final FileType fileType;

    /**
     * This is only applicable for ssm_p for now.
     */
    private final FileType metaFileType;

    /**
     * The client able to retrieve the ID from the service.
     */
    private IdentifierClient idClient;

    public Identify(String identifierClientClassName, String serviceUri, String releaseName, IdResolver idResolver,
        String projectId, FileType fileType, FileType metaFileType) {
      super(generatedFields(idResolver.getIdFieldName()));

      this.identifierClientClassName = checkNotNull(identifierClientClassName);
      this.serviceUri = checkNotNull(serviceUri);
      this.idResolver = checkNotNull(idResolver);
      this.projectId = checkNotNull(projectId);
      this.fileType = checkNotNull(fileType);
      this.metaFileType = metaFileType;
      this.releaseName = releaseName;
    }

    @Override
    public void prepare(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        OperationCall<T> operationCall) {
      super.prepare(flowProcess, operationCall);

      idClient = createIdentifierClient();
    }

    @Override
    public void operate(
        @SuppressWarnings("rawtypes") FlowProcess flowProcess,
        FunctionCall<T> functionCall) {
      TupleEntry entry = functionCall.getArguments();
      String id =
          checkNotNull(
              idResolver.getId(
                  projectId, entry, idClient, fileType, metaFileType),
              "Failed to get an ID for %s", entry);
      functionCall.getOutputCollector().add(new Tuple(id));
    }

    @SneakyThrows
    private IdentifierClient createIdentifierClient() {
      // Needs to have a constructor with a single String argument
      Class<?> identifierClientClass = Class.forName(identifierClientClassName);
      Constructor<?> constructor = identifierClientClass.getConstructor(String.class, String.class);

      log.info("Creating client using {}", serviceUri);
      return (IdentifierClient) constructor.newInstance(serviceUri, releaseName);
    }
  }

  /**
   * Helper enum for retrieve IDs.
   */
  @Slf4j
  @RequiredArgsConstructor
  public enum IdResolver {
    DONOR(DONOR_TYPE, IdentifierFieldNames.SURROGATE_DONOR_ID) {

      @Override
      public String getId(String projectId, TupleEntry entry, IdentifierClient idClient,
          FileType fileSchemaType,
          FileType metaFileSchemaType) {
        String originalDonorId = entry.getString(
            prefixedFields(fileSchemaType, SUBMISSION_DONOR_ID));
        printDebugLog(originalDonorId, projectId);
        return idClient.getDonorId(originalDonorId, projectId);
      }

    },
    SPECIMEN(SPECIMEN_TYPE, IdentifierFieldNames.SURROGATE_SPECIMEN_ID) {

      @Override
      public String getId(String projectId, TupleEntry entry, IdentifierClient idClient,
          FileType fileSchemaType,
          FileType metaFileSchemaType) {
        String originalSpecimenId = entry.getString(
            prefixedFields(fileSchemaType, SUBMISSION_SPECIMEN_ID));
        printDebugLog(originalSpecimenId, projectId);
        return idClient.getSpecimenId(originalSpecimenId, projectId);
      }

    },

    /**
     * Special case for the clinical hack (DCC-753), resolving donor_id from specimen rather than donor.
     */
    SPECIMEN_DONOR(SPECIMEN_TYPE, IdentifierFieldNames.SURROGATE_DONOR_ID) {

      @Override
      public String getId(String projectId, TupleEntry entry, IdentifierClient idClient,
          FileType fileSchemaType,
          FileType metaFileSchemaType) {
        String originalSpecimenId = entry.getString(
            prefixedFields(fileSchemaType, SUBMISSION_DONOR_ID));
        printDebugLog(originalSpecimenId, projectId);
        return idClient.getDonorId(originalSpecimenId, projectId);
      }

    },
    SAMPLE(SAMPLE_TYPE, IdentifierFieldNames.SURROGATE_SAMPLE_ID) {

      @Override
      public String getId(String projectId, TupleEntry entry, IdentifierClient idClient,
          FileType fileSchemaType,
          FileType metaFileSchemaType) {
        String originalSampleId = entry.getString(
            prefixedFields(fileSchemaType, SUBMISSION_ANALYZED_SAMPLE_ID));
        printDebugLog(originalSampleId, projectId);
        return idClient.getSampleId(originalSampleId, projectId);
      }

    },
    MUTATION(SSM_P_TYPE, IdentifierFieldNames.SURROGATE_MUTATION_ID) {

      @Override
      public String getId(String projectId, TupleEntry entry, IdentifierClient idClient,
          FileType fileSchemaType,
          FileType metaFileSchemaType) {
        checkState(metaFileSchemaType != null, "Expected to find a meta file schema name for %s", fileSchemaType);

        String chromosome = entry.getString(
            prefixedFields(fileSchemaType, SUBMISSION_OBSERVATION_CHROMOSOME));
        String chromosomeStart = entry.getString(
            prefixedFields(fileSchemaType, SUBMISSION_OBSERVATION_CHROMOSOME_START));
        String chromosomeEnd = entry.getString(
            prefixedFields(fileSchemaType, SUBMISSION_OBSERVATION_CHROMOSOME_END));
        String mutation = entry.getString(
            prefixedFields(fileSchemaType, NORMALIZER_MUTATION));
        String mutationType = entry.getString(
            prefixedFields(fileSchemaType, SUBMISSION_OBSERVATION_MUTATION_TYPE));
        String assemblyVersion = entry.getString(
            prefixedFields(metaFileSchemaType, SUBMISSION_OBSERVATION_ASSEMBLY_VERSION));

        printDebugLog(chromosome, chromosomeStart, chromosomeEnd, mutation, mutationType, assemblyVersion);
        return idClient.getMutationId(
            chromosome, chromosomeStart, chromosomeEnd, mutation, mutationType, assemblyVersion);
      }

    };

    @Getter
    private final FileType type;

    @Getter
    private final String idFieldName;

    /**
     * Returns the ID based on the kind of entity requiring it.
     * <p>
     * This basically extracts part of the business key from the {@link TupleEntry}, and combines it with the projectId
     * in order to query the service via the {@link HttpIdentifierClient}. The file schema name(s) are used to build
     * properly prefixed fields, as they are prefixed in the {@link TupleEntry}.
     */
    abstract String getId(String projectId, TupleEntry entry, IdentifierClient idClient,
        FileType fileSchemaType, FileType metaFileSchemaType);

    /**
     * Returns a pre-defined helper based on the file schema.
     */
    static IdResolver fromFileSchemaName(FileType type) {
      for (IdResolver idResolver : values()) {
        if (idResolver.type == type) {
          return idResolver;
        }
      }
      checkState(false, "Invalid file schema name: %s", type);
      return null;
    }

    private static void printDebugLog(String... values) {
      log.debug("Resolving ID for: {}", newArrayList(values));
    }
  }

}
