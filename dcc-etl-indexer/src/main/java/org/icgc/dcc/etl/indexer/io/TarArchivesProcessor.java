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
package org.icgc.dcc.etl.indexer.io;

import static org.icgc.dcc.common.hadoop.fs.FileOperations.getDataInputStream;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.exists;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.etl.indexer.model.DocumentType;

@Slf4j
@RequiredArgsConstructor
public class TarArchivesProcessor {

  /**
   * Dependencies.
   */
  @NonNull
  private final FileSystem fileSystem;

  /**
   * Configuration.
   */
  @NonNull
  private final String archiveDir;
  @NonNull
  private final String indexName;

  @SneakyThrows
  public void process(DocumentType type, TarArchiveEntryCallback callback) {
    val archiveFile = getArchivePath(type);

    if (!exists(fileSystem, archiveFile)) {
      log.warn("Archive file '{}' does not exist, skipping...", archiveFile);
      return;
    }

    @Cleanup
    val archiveInputStream = getDataInputStream(fileSystem, archiveFile);
    val reader = new TarArchiveDocumentReader(archiveInputStream);

    reader.read(type, callback);
  }

  private Path getArchivePath(DocumentType type) {
    return new Path(archiveDir, indexName + "-" + type.getName() + ".tar.gz");
  }

}
