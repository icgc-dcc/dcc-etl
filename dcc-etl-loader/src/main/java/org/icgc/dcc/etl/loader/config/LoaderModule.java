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
package org.icgc.dcc.etl.loader.config;

import static com.google.common.base.Preconditions.checkState;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.etl.loader.LoaderFileSystemProvider;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategyFactory;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategyFactoryProvider;
import org.icgc.dcc.etl.loader.service.LoaderService;
import org.icgc.dcc.common.hadoop.fs.DccFileSystem2;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

/**
 * Performs loader module DI configuration.
 */
public class LoaderModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(LoaderService.class).in(Singleton.class);

    bindNewTemporaryFileSystemAbstraction();
    bind(LoaderPlatformStrategyFactory.class)
        .toProvider(LoaderPlatformStrategyFactoryProvider.class)
        .in(Singleton.class);
    bind(Configuration.class).toInstance(new Configuration());
    bind(FileSystem.class).toProvider(LoaderFileSystemProvider.class).in(Singleton.class);
  }

  /**
   * Near clone of {@link ValidationModule} - maintain both at the same time until DCC-1876 is addressed.
   * <p>
   * TODO: This is temporary until DCC-1876 is addressed.
   * <p>
   * TODO: address hard-codings
   */
  private void bindNewTemporaryFileSystemAbstraction() {
    bind(DccFileSystem2.class).toProvider(new Provider<DccFileSystem2>() {

      @Inject
      private FileSystem fileSystem;

      @Inject
      private Config config;

      @Override
      public DccFileSystem2 get() {
        return new DccFileSystem2(fileSystem, getRootDir(), isHdfs());
      }

      private String getRootDir() {
        val path = "fs.root";
        checkState(config.hasPath(path), "'%s' should be present in the config", path);

        return config.getString(path);
      }

      private boolean isHdfs() {
        val path = "fs.url";
        checkState(config.hasPath(path), "'%s' should be present in the config", path);

        return config.getString(path).startsWith("hdfs");
      }

    });
  }
}
