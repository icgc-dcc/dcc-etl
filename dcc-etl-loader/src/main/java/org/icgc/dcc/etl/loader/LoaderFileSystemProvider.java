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
package org.icgc.dcc.etl.loader;

import static org.icgc.dcc.common.core.util.FsConfig.FS_URL;
import static org.icgc.dcc.common.hadoop.fs.Configurations.addFsDefault;
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getFileSystem;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.getConfigurationDescription;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.typesafe.config.Config;

@Slf4j
@RequiredArgsConstructor(onConstructor = @_(@Inject))
public class LoaderFileSystemProvider implements Provider<FileSystem> {

  @NonNull
  private final Config config;
  @NonNull
  private final Configuration configuration;

  @Override
  public FileSystem get() {
    val fs = getFileSystem(addFsDefault(configuration, config.getString(FS_URL)));
    log.info("Hadoop configuration: '{}'", getConfigurationDescription(fs.getConf()));

    return fs;
  }
}