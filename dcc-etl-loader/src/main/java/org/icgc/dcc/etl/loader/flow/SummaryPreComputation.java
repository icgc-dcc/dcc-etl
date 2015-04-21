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
import static org.icgc.dcc.etl.loader.flow.SummaryHelper.getDonorIdField;
import static org.icgc.dcc.etl.loader.flow.SummaryHelper.getSummaryTempDonorIdField;
import static org.icgc.dcc.etl.loader.flow.SummaryHelper.getSummaryValueField;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.ClinicalType;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.etl.loader.cascading.ControlledRowsRedacting;
import org.icgc.dcc.etl.loader.flow.planner.DonorRecordLoaderFlowPlanner;

import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;

@Slf4j
public class SummaryPreComputation extends SubAssembly {

  public SummaryPreComputation(
      @NonNull final Pipe observationPipe,
      @NonNull final String projectKey,
      @NonNull final FeatureType featureType,
      @NonNull final String summaryPipeName) {
    setTails(createSummaryPipe(
        observationPipe,
        projectKey,
        featureType,
        summaryPipeName));
  }

  public SummaryPreComputation(
      @NonNull final Pipe supplementalPipe,
      @NonNull final String projectKey,
      @NonNull final ClinicalType supplementalType,
      @NonNull final String summaryPipeName) {
    setTails(createSummaryPipe(
        supplementalPipe,
        projectKey,
        supplementalType,
        summaryPipeName));
  }

  /**
   * Creates an extra branch to provide summary information to the {@link DonorRecordLoaderFlowPlanner}.
   */
  // TODO Verify logic as applied for supplemental type
  private static Pipe createSummaryPipe(
      @NonNull final Pipe supplementalPipe,
      @NonNull final String projectKey,
      @NonNull final ClinicalType supplementalType,
      @NonNull final String summaryPipeName) {

    // Create branching pipe
    Pipe pipe = new Pipe(
        summaryPipeName,
        supplementalPipe);

    if (supplementalType.isCountSummary()) {
      pipe = new ControlledRowsRedacting(pipe);
    }

    // Only keep donor ID and rename it
    pipe = new Retain(pipe, getDonorIdField());
    pipe = new Rename(pipe, getDonorIdField(), getSummaryTempDonorIdField());

    // Summarise based on the type (count or presence)
    if (supplementalType.isCountSummary()) {
      log.info("Counting by donors for {} ({})", projectKey, supplementalType);

      // Count by donors
      pipe = new CountBy(
          pipe,
          getSummaryTempDonorIdField(),
          getSummaryValueField(supplementalType));
    } else {
      log.info("Uniquifying donors for {} ({})", projectKey, supplementalType);

      // Uniquify donors
      pipe = new Unique(pipe, getSummaryTempDonorIdField());

      // Mark the resulting tuples as present (to distinguish them when we side join later on)
      pipe = new Each(pipe,
          new Insert(
              getSummaryValueField(supplementalType),
              true),
          ALL);
    }

    return pipe;
  }

  /**
   * Creates an extra branch to provide summary information to the {@link DonorRecordLoaderFlowPlanner}.
   */
  private static Pipe createSummaryPipe(
      @NonNull final Pipe observationPipe,
      @NonNull final String projectKey,
      @NonNull final FeatureType featureType,
      @NonNull final String summaryPipeName) {

    // Create branching pipe
    Pipe pipe = new Pipe(
        summaryPipeName,
        observationPipe);

    if (featureType.isCountSummary()) {
      pipe = new ControlledRowsRedacting(pipe);
    }

    // Only keep donor ID and rename it
    pipe = new Retain(pipe, getDonorIdField());
    pipe = new Rename(pipe, getDonorIdField(), getSummaryTempDonorIdField());

    // Summarise based on the type (count or presence)
    if (featureType.isCountSummary()) {
      log.info("Counting by donors for {} ({})", projectKey, featureType);

      // Count by donors
      pipe = new CountBy(
          pipe,
          getSummaryTempDonorIdField(),
          getSummaryValueField(featureType));
    } else {
      log.info("Uniquifying donors for {} ({})", projectKey, featureType);

      // Uniquify donors
      pipe = new Unique(pipe, getSummaryTempDonorIdField());

      // Mark the resulting tuples as present (to distinguish them when we side join later on)
      pipe = new Each(pipe,
          new Insert(
              getSummaryValueField(featureType),
              true),
          ALL);
    }

    return pipe;
  }

}
