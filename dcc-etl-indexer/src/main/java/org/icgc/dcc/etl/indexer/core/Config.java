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
package org.icgc.dcc.etl.indexer.core;

import java.io.File;
import java.io.Serializable;
import java.util.Map;

import lombok.NonNull;
import lombok.Value;
import lombok.Builder;

@Value
@Builder
public class Config implements Serializable {

  /**
   * The name of the release.
   */
  @NonNull
  String releaseName;

  /**
   * Input collections.
   */
  @NonNull
  String mongoUri;

  /**
   * The output ES URI.
   */
  @NonNull
  String esUri;

  /**
   * The output {@link FileSystem} URI.
   */
  @NonNull
  String fsUri;

  /**
   * The output ESindex name.
   */
  @NonNull
  String indexName;

  /**
   * The output archive dir.
   */
  @NonNull
  String outputDir;

  /**
   * The input FASTA file.
   */
  @NonNull
  File fastaFile;

  /**
   * Whether to export mutation VCF file.
   */
  boolean exportVCF;

  /**
   * Whether to optimize the index.
   */
  boolean optimize;

  /**
   * Hadoop properties.
   */
  Map<String, String> hadoop;

}
