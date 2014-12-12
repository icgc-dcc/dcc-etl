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
package org.icgc.dcc.etl.indexer.cli;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

import java.io.File;
import java.util.List;
import java.util.Map;

import lombok.ToString;

import org.icgc.dcc.etl.indexer.model.DocumentType;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;

/**
 * Command line options.
 */
@ToString
public class Options {

  /**
   * Meta - Release
   */
  @Parameter(names = { "-r", "--release-name" }, required = true, description = "The ICGC release name (e.g. ICGC15")
  public String releaseName;

  /**
   * Input - MongoDB
   */
  @Parameter(names = { "-m", "--mongo-uri" }, required = true, validateWith = MongoURIValidator.class, validateValueWith = MongoValidator.class, description = "The MongoDB URI (e.g. mongodb://localhost:27017")
  public String mongoUri = "mongodb://localhost:27017";
  @Parameter(names = { "-d", "--database-name" }, required = true, description = "The MongoDB database name (e.g. ICGC15")
  public String databaseName;

  /**
   * Input - FASTA
   */
  @Parameter(names = { "-f", "--fasta-file" }, required = true, validateValueWith = FileValidator.class, description = "Input FASTA file (e.g. /tmp/GRCh37.fasta)")
  public File fastaFile = new File("/tmp/GRCh37.fasta");

  /**
   * Output - Elasticsearch
   */
  @Parameter(names = { "-e", "--es-uri" }, required = true, description = "The Elasticsearch URI (e.g. es://localhost:9300)")
  public String esUri = "es://localhost:9300";
  @Parameter(names = { "-i", "--index-name" }, required = true, description = "The name of the index to create (e.g. ICGC15)")
  public String indexName;

  /**
   * Output - Archive / VCF
   */
  @Parameter(names = { "-o", "--output-dir" }, required = true, description = "Output local or HDFS archive and VCF directory (e.g. /tmp, /icgc/releases)")
  public String outputDir = "/tmp";

  /**
   * Behavior
   */
  @Parameter(names = { "-l", "--local" }, description = "Process locally (default is false)")
  public boolean local;
  @Parameter(names = { "-t", "--types" }, converter = DocumentTypeConverter.class, description = "Document types to create. Comma seperated list of: 'donor', 'donor-centric', 'gene-centric', 'gene-project', 'project', 'observation-centric', 'mutation-centric'. By default all index types will be created.")
  public List<DocumentType> types = newArrayList(DocumentType.values());
  @Parameter(names = { "--export-vcf" }, description = "Export VCF to 'outputDir'?")
  public boolean exportVCF = true;

  /**
   * Hadoop
   */
  @DynamicParameter(names = "-D", description = "Hadoop properties")
  public Map<String, String> hadoop = newHashMap();

  /**
   * Info
   */
  @Parameter(names = { "-v", "--version" }, help = true, description = "Show version information")
  public boolean version;
  @Parameter(names = { "-h", "--help" }, help = true, description = "Show help information")
  public boolean help;

}