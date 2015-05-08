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
package org.icgc.dcc.etl.db.importer.repo.core;

import static org.icgc.dcc.common.core.model.ReleaseCollection.FILE_COLLECTION;
import static org.icgc.dcc.etl.db.importer.util.Jongos.createJongo;

import java.util.function.Consumer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.etl.db.importer.util.TransportClientFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoClientURI;

@RequiredArgsConstructor
public class RepositoryFileIndexer {

  /**
   * Constants.
   */
  private static String INDEX_NAME = "icgc-repository";
  private static String INDEX_TYPE_NAME = "file";

  /**
   * Configuration
   */
  @NonNull
  private final MongoClientURI mongoUri;
  @NonNull
  private final String esUri;

  public void indexFiles() {
    val client = TransportClientFactory.newTransportClient(esUri);
    client.prepareDelete().setIndex(INDEX_NAME).execute();

    eachFile(file -> {
      // Need to remove this as to not conflict with Elasticsearch
      file.remove("_id");

      String source = file.toString();
      client.prepareIndex(INDEX_NAME, INDEX_TYPE_NAME).setSource(source).execute();
    });
  }

  private void eachFile(Consumer<ObjectNode> fileCallback) {
    val jongo = createJongo(mongoUri);
    val files = jongo.getCollection(FILE_COLLECTION.getId());
    for (val file : files.find().as(ObjectNode.class)) {
      fileCallback.accept(file);
    }
  }

}
