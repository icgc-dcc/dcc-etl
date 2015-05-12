/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.repo.core;

import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.collect.Iterables.isEmpty;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import lombok.Cleanup;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.repo.model.RepositoryFile;
import org.icgc.dcc.etl.repo.model.RepositorySource;

@Slf4j
@RequiredArgsConstructor
public abstract class RepositorySourceFileImporter {

  /**
   * Metadata.
   */
  @NonNull
  @Getter
  protected RepositorySource source;

  /**
   * Dependencies.
   */
  @NonNull
  protected RepositoryFileContext context;

  @SneakyThrows
  public void execute() {
    val watch = createStarted();

    log.info("Reading files...");
    val files = readFiles();
    log.info("Finished reading files");

    if (isEmpty(files)) {
      log.error("**** Files are empty! Reusing previous imported files");
      return;
    }

    log.info("Writing files...");
    writeFiles(files);
    log.info("Finished writing files");

    log.info("Imported {} files in {}.", formatCount(files), watch);
  }

  protected abstract Iterable<RepositoryFile> readFiles();

  @SneakyThrows
  protected void writeFiles(Iterable<RepositoryFile> files) {
    @Cleanup
    val writer = new RepositorySourceFileWriter(context.getMongoUri(), source);
    writer.write(files);
  }

}
