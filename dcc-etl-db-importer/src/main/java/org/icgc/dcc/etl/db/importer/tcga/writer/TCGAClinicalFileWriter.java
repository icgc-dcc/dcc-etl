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
package org.icgc.dcc.etl.db.importer.tcga.writer;

import static org.icgc.dcc.common.core.model.ReleaseCollection.FILE_COLLECTION;
import static org.icgc.dcc.etl.db.importer.file.util.FileRepositories.FILE_REPOSITORY_FIELD_NAME;
import static org.icgc.dcc.etl.db.importer.file.util.FileRepositories.FILE_REPOSITORY_TCGA_VALUE;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.tcga.model.TCGAClinicalFile;
import org.icgc.dcc.etl.db.importer.util.AbstractJongoWriter;
import org.jongo.MongoCollection;

import com.mongodb.MongoClientURI;

@Slf4j
public class TCGAClinicalFileWriter extends AbstractJongoWriter<Iterable<TCGAClinicalFile>> {

  public TCGAClinicalFileWriter(@NonNull MongoClientURI mongoUri) {
    super(mongoUri);
  }

  @Override
  public void write(@NonNull Iterable<TCGAClinicalFile> clinicalFiles) {
    log.info("Clearing file documents...");
    val fileCollection = getCollection(FILE_COLLECTION);
    clearFiles(fileCollection);

    log.info("Writing file documents...");
    for (val clinicalFile : clinicalFiles) {
      fileCollection.save(clinicalFile);
    }
  }

  private static void clearFiles(MongoCollection fileCollection) {
    val result = fileCollection.remove("{ " + FILE_REPOSITORY_FIELD_NAME + ": # }", FILE_REPOSITORY_TCGA_VALUE);

    log.info("Finished clearing collection '{}': {}", fileCollection, result);
  }

}
