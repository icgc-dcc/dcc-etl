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
package org.icgc.dcc.etl.client.cli;

import static com.google.common.collect.Lists.newArrayList;

import java.io.File;
import java.util.List;

import lombok.ToString;

import org.icgc.dcc.etl.indexer.model.DocumentType;

import com.beust.jcommander.Parameter;

/**
 * Command line options.
 */
@ToString
public class Options {

  @Parameter(description = "Processes to execute. Leave empty to execute all or specify one or more values:  org.icgc.dcc.etl.service.EtlService.Component.values()*.name().toLowerCase()")
  public final List<String> processes = newArrayList();

  @Parameter(names = { "--config" }, required = true, validateValueWith = FileValidator.class, description = "Configuration file (e.g. ./config.yaml)")
  public File config;

  @Parameter(names = { "--job-id" }, required = true, description = "The job ID for the current run")
  public String jobId;

  @Parameter(names = { "--release-prefix" }, required = true, description = "The release prefix (typically 'ICGC' for real runs)")
  public String releasePrefix;

  @Parameter(names = { "--release-number" }, required = true, description = "The release number")
  public int releaseNumber;

  @Parameter(names = { "--patch-number" }, required = true, description = "The patch number (when a release needs to be re-processed)")
  public int patchNumber;

  @Parameter(names = { "--run-number" }, required = true, description = "The run number (to be assigned automatically by the parent ETL script)")
  public int runNumber;

  @Parameter(names = { "--dictionary" }, required = true, description = "The ICGC dictionary to use as metadata for the ETL process")
  public String dictionaryFilePath;

  @Parameter(names = { "--codelists" }, required = true, description = "The ICGC codelists to use as metadata for the ETL process")
  public String codeListsFilePath;

  @Parameter(names = { "--working-dir" }, required = true, description = "The working dir under which components are nested (themselves nesting projects)")
  public String workingDir;

  @Parameter(names = { "--projects" }, required = true, description = "The list of projects to process")
  public List<String> projects = newArrayList();

  @Parameter(names = { "--index-types" }, converter = DocumentTypeConverter.class, description = "Index types to CREATE. Comma seperated list of: 'donor', 'donor-centric', 'gene-centric', 'gene-project', 'project', 'observation-centric', 'mutation-centric'. By default all index types will be created.")
  public List<DocumentType> types = newArrayList();

  @Parameter(names = { "--alias" }, description = "Alias the produced index with the specified value after first removing the alias from all existing indexes")
  public String alias;

  @Parameter(names = { "-v", "--version" }, help = true, description = "Show version information")
  public boolean version;

  @Parameter(names = { "-h", "--help" }, help = true, description = "Show help information")
  public boolean help;

}
