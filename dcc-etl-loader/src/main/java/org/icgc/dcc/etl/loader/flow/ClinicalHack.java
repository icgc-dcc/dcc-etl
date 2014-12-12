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

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SAMPLE_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SPECIMEN_TYPE;
import static org.icgc.dcc.common.core.util.Optionals.ABSENT_FILE_TYPE;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.generatedFields;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;
import static org.icgc.dcc.etl.loader.identification.Identifier.identify;
import static org.icgc.dcc.common.hadoop.cascading.Fields2.getCountFieldCounterpart;
import lombok.NonNull;

import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.model.Identifiable;
import org.icgc.dcc.etl.loader.identification.Identifier;

import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.tuple.Fields;

/**
 * Still part of the DCC-753 hack.
 */
public class ClinicalHack extends SubAssembly {

  public ClinicalHack(
      @NonNull final Pipe samplePipe,
      @NonNull final Pipe specimenPipe,
      @NonNull final String releaseName,
      @NonNull final Identifiable projectKey,
      @NonNull final String identifierUri,
      @NonNull final String identifierClientClassName) {
    setTails(process(
        samplePipe, specimenPipe,
        releaseName, projectKey,
        identifierUri, identifierClientClassName));
  }

  private static Pipe process(
      @NonNull final Pipe preProcessedSamplePipe,
      @NonNull final Pipe preProcessedSpecimenPipe,
      @NonNull final String releaseName,
      @NonNull final Identifiable projectKey,
      @NonNull final String identifierUri,
      @NonNull final String identifierClientClassName) {

    // Retain only fields of interest
    Pipe samplePipe = new Retain(preProcessedSamplePipe,
        sampleSpecimenLeftJoinFields()
            .append(observationClinicalTumourRightJoinFields()));
    Pipe specimenPipe = new Retain(preProcessedSpecimenPipe,
        donorIdField()
            .append(sampleSpecimenRightJoinFields()));

    // Fetch ID for sample
    samplePipe = identify(
        identifierClientClassName, identifierUri, releaseName, samplePipe, projectKey.getId(),
        SAMPLE_TYPE, ABSENT_FILE_TYPE, Identifier.IdResolver.SAMPLE);

    // Fetch ID for donor and specimen
    specimenPipe = identify( // We use donor_id from specimen rather than from donor (to save a join)
        identifierClientClassName, identifierUri, releaseName, specimenPipe, projectKey.getId(),
        SPECIMEN_TYPE, ABSENT_FILE_TYPE, Identifier.IdResolver.SPECIMEN_DONOR);
    specimenPipe = identify(
        identifierClientClassName, identifierUri, releaseName, specimenPipe, projectKey.getId(),
        SPECIMEN_TYPE, ABSENT_FILE_TYPE, Identifier.IdResolver.SPECIMEN);

    // Perform join for tumour sample ID
    Pipe clinicalPipe = new HashJoin(
        samplePipe, sampleSpecimenLeftJoinFields(),
        specimenPipe, sampleSpecimenRightJoinFields(),
        new InnerJoin());

    // Retain only fields of interest
    clinicalPipe = new Retain(clinicalPipe,
        generatedFields(DONOR_ID, DONOR_SPECIMEN_ID, DONOR_SAMPLE_ID)
            .append(observationClinicalTumourRightJoinFields())); // For reuse as mentioned above

    return clinicalPipe;
  }

  public static Fields donorIdField() {
    return prefixedFields(SPECIMEN_TYPE, SUBMISSION_DONOR_ID);
  }

  public static Fields sampleSpecimenRightJoinFields() {
    return prefixedFields(SPECIMEN_TYPE, SUBMISSION_SPECIMEN_ID);
  }

  public static Fields sampleSpecimenLeftJoinFields() {
    return prefixedFields(SAMPLE_TYPE, SUBMISSION_SPECIMEN_ID);
  }

  public static Fields observationClinicalTumourRightJoinFields() {
    return prefixedFields(SAMPLE_TYPE, SUBMISSION_ANALYZED_SAMPLE_ID);
  }

  public static Fields observationClinicalObservationControlRightJoinFields() {
    return prefixedFields(
        SAMPLE_TYPE,
        getCountFieldCounterpart(SUBMISSION_ANALYZED_SAMPLE_ID)); // Temporary name (discarded soon after)
  }

  public static Fields observationClinicalLeftJoinFields(FileType fileType) {
    return prefixedFields(fileType, SUBMISSION_ANALYZED_SAMPLE_ID);
  }

}
