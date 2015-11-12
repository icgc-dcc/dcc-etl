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
package org.icgc.dcc.etl.pruner;

import static org.icgc.dcc.common.json.Jackson.DEFAULT;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.common.hadoop.fs.FileSystems;
import org.icgc.dcc.etl.core.config.EtlConfigFile;
import org.icgc.dcc.submission.dictionary.model.Dictionary;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;

public class PrunerMain {

  public static void main(String[] args) throws IOException {
    val options = new Options();
    new JCommander(options, args);

    if (options.remove) {
      new Pruner(
          getFileSystem(options.configFilePath),
          getDictionary(options.dictionaryFilePath))
              .prune(options.inputDirectory, options.projectsJsonFilePath);
    } else {
      new Reporter(
          getFileSystem(options.configFilePath),
          getDictionary(options.dictionaryFilePath))
              .report(options.inputDirectory, options.projectsJsonFilePath);
    }

  }

  private static FileSystem getFileSystem(String configFilePath) {
    return FileSystems.getFileSystem(
        EtlConfigFile
            .read(new File(configFilePath))
            .getFsUrl());
  }

  @SneakyThrows
  private static Dictionary getDictionary(String dictionaryFilePath) {
    Dictionary dictionary = DEFAULT.reader(Dictionary.class).readValue(new File(dictionaryFilePath));
    return dictionary;
  }

  @ToString
  public static class Options {

    @Parameter(names = { "-i", "--input" }, required = true, description = "Input directory for projects (for instance \"/icgc/submission/ICGC16\")")
    public String inputDirectory;
    @Parameter(names = { "-p", "--projects" }, required = true, description = "Project description file (see dcc-etl/dcc-etl-client/src/test/resources/fixtures/concatenator/projects.json for an example)")
    public String projectsJsonFilePath;
    @Parameter(names = { "-d", "--dictionary" }, required = true, description = "Path to dictionary")
    public String dictionaryFilePath;
    @Parameter(names = { "-c", "--config" }, required = true, description = "Path to the ETL config file")
    public String configFilePath;
    @Parameter(names = { "-r", "--remove" }, required = false, description = "If set, removes orphaned data")
    public boolean remove = false;

  }
}
