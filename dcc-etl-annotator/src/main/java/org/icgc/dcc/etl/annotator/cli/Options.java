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
package org.icgc.dcc.etl.annotator.cli;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import lombok.ToString;

import org.icgc.dcc.etl.annotator.model.AnnotatedFileType;

import com.beust.jcommander.Parameter;

/**
 * Command line options.
 */
@ToString
public class Options {

  @Parameter(names = { "--working-dir" }, required = true, description = "The base working dir from which to read SSM / SGV primary files from and annotate SSM / SGV secondary files to")
  public String workingDir;

  @Parameter(names = { "--project-names" }, description = "The list of project names / codes to process. Defaults to all")
  public List<String> projectNames = newArrayList();

  @Parameter(names = { "--file-types" }, converter = AnnotatedFileTypeConverter.class, description = "File types to process: ssm or sgv. Defaults to both")
  public List<AnnotatedFileType> fileTypes = newArrayList(AnnotatedFileType.values());

  @Parameter(names = { "-v", "--version" }, help = true, description = "Show version information")
  public boolean version;

  @Parameter(names = { "-h", "--help" }, help = true, description = "Show help information")
  public boolean help;

}
