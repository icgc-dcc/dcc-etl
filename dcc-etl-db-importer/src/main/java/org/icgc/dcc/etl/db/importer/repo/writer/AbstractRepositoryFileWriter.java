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
package org.icgc.dcc.etl.db.importer.repo.writer;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.model.ReleaseCollection.FILE_COLLECTION;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.db.importer.repo.model.FileRepositories.FILE_REPOSITORY_TYPE_FIELD_NAME;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.repo.model.FileRepositoryOrg;
import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;
import org.icgc.dcc.etl.db.importer.util.AbstractJongoWriter;
import org.jongo.MongoCollection;

import com.mongodb.MongoClientURI;

@Slf4j
public abstract class AbstractRepositoryFileWriter<T> extends AbstractJongoWriter<T> {

  /**
   * Dependencies.
   */
  @NonNull
  protected final MongoCollection fileCollection;

  /**
   * Dependencies.
   */
  @NonNull
  protected final FileRepositoryOrg type;

  public AbstractRepositoryFileWriter(@NonNull MongoClientURI mongoUri, @NonNull FileRepositoryOrg type) {
    super(mongoUri);
    this.fileCollection = getCollection(FILE_COLLECTION);
    this.type = type;
  }

  public void clearFiles() {
    log.info("Clearing '{}' documents in collection '{}'", type.getId(), fileCollection.getName());
    val result = fileCollection.remove("{ " + FILE_REPOSITORY_TYPE_FIELD_NAME + ": # }", type.getId());
    checkState(result.getLastError().ok(), "Error clearing mongo: %s", result);

    log.info("Finished clearing {} '{}' documents in collection '{}'",
        formatCount(result.getN()), type.getId(), fileCollection.getName());
  }

  protected void saveFile(RepositoryFile file) {
    fileCollection.save(file);
  }

}
