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
package org.icgc.dcc.etl.loader.platform;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DataType;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.etl.loader.core.ProvidedDataReleaseDigest;
import org.icgc.dcc.etl.loader.flow.SummaryCollector;

import cascading.flow.FlowConnector;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Abstraction to the platform that the loader executes against. This is similar to the {@code cascading-platform} test
 * module or {@code cascading.bind} extension module that Cascading offers.
 * 
 * @see https://github.com/cwensel/cascading/tree/wip-2.1/cascading-platform
 * @see https://github.com/cwensel/cascading.bind
 */
public interface LoaderPlatformStrategy {

  /**
   * Initialises the file system, such as creating parent directory and deleting output file (so we can append to them
   * on a per project basis).
   */
  void initialise(ProvidedDataReleaseDigest dataDigest);

  /**
   * Returns the output directory for a given type and under {@link #getDataTypeOutputDir(DataType)}.
   */
  Path getDataTypeOutputDir(DataType type);

  /**
   * Map of project to (file type -> file path) map.
   */
  Map<String, Map<FileType, String>> getInputFiles();

  /**
   * Returns the size in bytes of a given submission, computed as the sum of the size of each of the files it contains.
   */
  long size(String submission);

  /**
   * Returns a platform-specific {@code FlowConnector}.
   */
  FlowConnector getFlowConnector();

  /**
   * Necessary until DCC-996 is done (IF there is indeed a more elegant alternative).
   */
  Fields getFileHeader(String submission, FileType fileType);

  /**
   * Returns a platform-specific submission source tap.
   */
  Tap<?, ?, ?> getSourceTap(String submission, FileType fileType);

  /**
   * Returns an intermediate platform-specific tap for summary information.
   */
  Tap<?, ?, ?> getSummaryTap(String submission, SummaryCollector.SummaryTapInfo summaryTap);

  /**
   * Returns file system tap for exports, on a per submission/per schema basis. At the end of the run, the resulting
   * files for each submissions will be concatenated to make up one such file per schema. TODO: update
   */
  Tap<?, ?, ?> getFileSystemTap(DataType type, String submission);

  /**
   * Returns a platform-specific database sink tap.
   */
  Tap<?, ?, ?> getDatabaseTap(DataType dataType);

  /**
   * Finalises the file system, for instance cleaning up working directories.
   */
  void finalise(ProvidedDataReleaseDigest dataDigest);

}
