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
package org.icgc.dcc.etl.loader.cascading;

import static cascading.tuple.Fields.ARGS;
import static cascading.tuple.Fields.REPLACE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_DIGITAL_IMAGE_OF_STAINED_SECTION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SPECIMEN_TYPE;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;
import lombok.val;

import org.icgc.dcc.common.hadoop.cascading.operation.BaseFunction;
import org.icgc.dcc.etl.loader.util.StainedSectionImageUrlResolver;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This modifies the digital_image_of_stained_section field in donor specimen to a real URL, if applicable;
 */
public class StainedSectionImageUpdate extends SubAssembly {

  /**
   * State.
   */
  private static final StainedSectionImageUrlResolver STAINED_IMAGE_URL_RESOLVER =
      new StainedSectionImageUrlResolver(true); // False means do not validate because the server is currently unstable

  /**
   * Fields.
   */
  private static final Fields SPECIMEN_ID_FIELD =
      prefixedFields(SPECIMEN_TYPE, SUBMISSION_SPECIMEN_ID);
  private static final Fields DIGITAL_IMAGE_OF_STAINED_SECTION_FIELD =
      prefixedFields(SPECIMEN_TYPE, SUBMISSION_DIGITAL_IMAGE_OF_STAINED_SECTION);
  private static final Fields ARGUMENT_FIELDS =
      SPECIMEN_ID_FIELD.append(DIGITAL_IMAGE_OF_STAINED_SECTION_FIELD);

  public StainedSectionImageUpdate(Pipe pipe) {
    setTails(new Each(pipe, ARGUMENT_FIELDS, resolveSpecimenImageUrl(), REPLACE));
  }

  private static Function<Void> resolveSpecimenImageUrl() {
    return new BaseFunction<Void>(ARGUMENT_FIELDS.size(), ARGS) {

      @Override
      public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess, FunctionCall<Void> call) {
        val specimenId = call.getArguments().getString(SPECIMEN_ID_FIELD);
        val specimenUrl = STAINED_IMAGE_URL_RESOLVER.resolveUrl(specimenId);

        // Append field
        call.getOutputCollector().add(new Tuple(specimenId, specimenUrl));
      }

    };
  }

}
