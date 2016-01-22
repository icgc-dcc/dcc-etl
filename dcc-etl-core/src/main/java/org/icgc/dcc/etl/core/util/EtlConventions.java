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
package org.icgc.dcc.etl.core.util;

import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.Component.CONCATENATOR;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import lombok.NoArgsConstructor;
import lombok.NonNull;

import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.etl.core.model.Component;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

@NoArgsConstructor(access = PRIVATE)
public final class EtlConventions {

  public static final Joiner JOB_ID_JOINER = Joiners.DASH;

  public static final Iterable<Entry<FileType, Component>> GENERATED_FILE_TYPE_TO_COMPONENT = ImmutableList.of(
      getEntry(FileType.SSM_P_TYPE, Component.NORMALIZER),
      getEntry(FileType.SGV_P_TYPE, Component.NORMALIZER),
      getEntry(FileType.SSM_S_TYPE, Component.ANNOTATOR),
      getEntry(FileType.SGV_S_TYPE, Component.ANNOTATOR));

  public static String getConcatenatorProjectInputDir(
      @NonNull final String workingDir,
      @NonNull final String projectKey) {
    return CONCATENATOR.getProjectDir(workingDir, projectKey);
  }

  public static String getInputGeneratingComponentProjectInputDir(
      @NonNull final Component inputGeneratingComponent,
      @NonNull final String workingDir,
      @NonNull final String projectKey) {
    checkState(inputGeneratingComponent.isEtlInputGenerating());
    return inputGeneratingComponent.getProjectDir(workingDir, projectKey);
  }

  public static String getLoaderOutputDir(@NonNull final String workingDir) {
    return Component.LOADER.getComponentDir(workingDir);
  }

  public static String getIndexerOutputDir(@NonNull final String workingDir) {
    return Component.INDEXER.getComponentDir(workingDir);
  }

  private static Entry<FileType, Component> getEntry(FileType fileType, Component component) {
    return new SimpleEntry<FileType, Component>(fileType, component);
  }

}
