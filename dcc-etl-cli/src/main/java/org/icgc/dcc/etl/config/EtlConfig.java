package org.icgc.dcc.etl.config;

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

import java.io.File;
import java.util.Map;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EtlConfig {

  /**
   * Scheme for the filesystem, e.g. file:// or hdfs://.
   * <p>
   * TODO: rename to fsScheme...
   */
  String fsUrl;

  /**
   * Compression to use for the filesystem output of the loader (see HadoopCompression enum).
   */
  String fileSystemOutputCompression;

  String esUri;

  String releaseMongoUri;

  String etlAdminMongoUri;

  String projectMongoUri;

  String geneMongoUri;

  File summaryDir;

  /**
   * Storage location of FASTA file.
   */
  File fastaFile;

  /**
   * Temporary file cache location.
   */
  File cacheDir = new File("/tmp");

  /**
   * URL of the indentifier web server.
   */
  String identifierServiceUri;

  String fathmmPostgresqlUri;

  /**
   * Whether to filter out CONTROLLED rows for all or just for the portal (default is for all, out of safety).
   */
  boolean filterAllControlled = true;

  /**
   * Whether to export mutation VCF file.
   */
  boolean exportVCF = true;

  /**
   * The number of maximum concurrent flows in the loader phase.
   */
  int loaderMaxConcurrentFlows = 100;

  /**
   * Hadoop properties for the loader.
   */
  Map<String, String> loaderHadoop;

  /**
   * Hadoop properties for the indexers.
   */
  Map<String, String> indexerHadoop;

  /**
   * ICGC client configuration
   */
  Map<String, String> icgc;

}
