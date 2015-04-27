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
package org.icgc.dcc.etl.db.importer.cghub.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.ByteStreams.copy;
import static java.lang.String.format;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CGHubCache {

  /**
   * The extension placed on cache files.
   */
  private static final String CACHE_FILE_EXTENSION = "xml";

  /**
   * The local path to the cache of CGHub XML responses.
   */
  @NonNull
  private final File cacheDir;

  public CGHubCache(File cacheDir) {
    this.cacheDir = cacheDir;
    checkArgument(cacheDir.exists(), "The specified CGHub file cache path '%s' does not exist.", cacheDir);
    checkArgument(cacheDir.isDirectory(), "The specified CGHub file cache path '%s' is not a directory.", cacheDir);
  }

  public void cacheFile(InputStream input, String projectId) throws IOException {
    val cachedFile = cachedFile(projectId);
    val output = new FileOutputStream(cachedFile);

    log.info("Caching project '{}' to '{}'...", projectId, cachedFile);
    copy(input, output);
  }

  public InputStream readCache(String project) throws FileNotFoundException {
    return new FileInputStream(cachedFile(project));
  }

  public boolean isCached(String projectId) {
    return cachedFile(projectId).exists();
  }

  private File cachedFile(String projectId) {
    val fileName = format("%s.%s", projectId, CACHE_FILE_EXTENSION);

    return new File(cacheDir, fileName);
  }

}
