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

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.model.Identifiable;
import org.icgc.dcc.common.core.model.Identifiable.Identifiables;
import org.icgc.dcc.etl.loader.cascading.RawSequenceDataInfo;
import org.icgc.dcc.etl.loader.core.LoaderTailType;
import org.icgc.dcc.etl.loader.flow.ClinicalHack;
import org.icgc.dcc.etl.loader.flow.SummaryCollector;
import org.icgc.dcc.common.cascading.Pipes;

@NoArgsConstructor(access = PRIVATE)
public final class PipeNames {

  public static String getStartPipeName(
      @NonNull final Identifiable projectKey,
      @NonNull final FileType fileType) {
    return Pipes.getName(Identifiables.fromString(BaseRecordLoaderFlowPlanner.class.getSimpleName()), projectKey,
        fileType);
  }

  public static String getEndPipeName(
      @NonNull final Identifiable projectKey,
      @NonNull final LoaderTailType tailType) {
    return Pipes.getName(Identifiables.fromString(BaseRecordLoaderFlowPlanner.class.getSimpleName()), projectKey,
        tailType);
  }

  public static String getClinicalHackPipeName(
      @NonNull final Identifiable projectKey,
      @NonNull final FileType fileType) {
    return Pipes.getName(Identifiables.fromString(ClinicalHack.class.getSimpleName()), projectKey, fileType);
  }

  public static String getSummaryPipeName(
      @NonNull final Identifiable projectKey,
      @NonNull final FeatureType featureType) {
    return Pipes.getName(Identifiables.fromString(SummaryCollector.class.getSimpleName()), projectKey, featureType);
  }

  public static String getRawSequenceDataInfoPipeName(
      @NonNull final Identifiable projectKey,
      @NonNull final FeatureType featureType) {
    return Pipes.getName(Identifiables.fromString(RawSequenceDataInfo.class.getSimpleName()), projectKey, featureType);
  }

}
