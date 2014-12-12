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
package org.icgc.dcc.etl.core.model;

import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.etl.core.model.EtlInputGeneration.GENERATING;
import static org.icgc.dcc.etl.core.model.EtlInputGeneration.NA;
import static org.icgc.dcc.etl.core.model.EtlInputGeneration.REWRITTING;
import static org.icgc.dcc.etl.core.model.EtlInputGeneration.WRITTING;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.icgc.dcc.common.core.model.Identifiable;

/**
 * Represents components in our system.
 * <p>
 * TODO: move to {@link EtlConventions}?
 */
@RequiredArgsConstructor
public enum Component implements Identifiable {

  /**
   * Rewrites all submission files.
   */
  CONCATENATOR(WRITTING),

  /**
   * Rewrites {@link FileType#SSM_P_TYPE} file.
   */
  NORMALIZER(REWRITTING),

  /**
   * Generates {@link FileType#SSM_S_TYPE} file.
   */
  ANNOTATOR(GENERATING),

  LOADER(NA),
  IMPORTER(NA),
  SUMMARIZER(NA),
  INDEXER(NA),
  STATS(NA),

  EXPORTER(NA),

  /**
   * TODO: keep?
   */
  REPORTER(NA),
  ETL(NA);

  /**
   * Whether the component writes/rewrites ETL input files.
   */
  private final EtlInputGeneration etlInputGeneration;

  @Override
  public String getId() {
    return name().toLowerCase();
  }

  public String getDirName() {
    return getId();
  }

  public boolean isEtlInputGenerating() {
    return etlInputGeneration.isGenerating();
  }

  /**
   * Do not use outside of {@link EtlConventions}.
   */
  public String getComponentDir(@NonNull final String workingDir) {
    return PATH.join(
        workingDir,
        getDirName());
  }

  /**
   * Do not use outside of {@link EtlConventions}.
   */
  public String getProjectDir(
      @NonNull final String workingDir,
      @NonNull final String projectKey) {
    return PATH.join(
        getComponentDir(workingDir),
        projectKey);
  }

}
