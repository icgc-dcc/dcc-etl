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
package org.icgc.dcc.etl.db.importer.cghub.writer;

import java.net.UnknownHostException;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.jongo.Jongo;
import org.jongo.MongoCollection;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

@Slf4j
public class CGHubWriter {

  /**
   * The collection name where records will be store.
   */
  private static final String COLLECTION_NAME = "CGHub";

  @NonNull
  private final String mongoUri;
  @NonNull
  private final Jongo jongo;
  @NonNull
  private final MongoCollection collection;

  public CGHubWriter(String mongoUri) {
    this.mongoUri = mongoUri;
    this.jongo = createJongo(mongoUri);
    this.collection = getCollection(jongo);
  }

  public void insert(DBObject record) {
    log.info("Inserting {}...", record);
    collection.save(record);
  }

  public long getCount() {
    return collection.count();
  }

  public void drop() {
    log.info("Dropping collection '{}' from '{}'...", collection.getName(), mongoUri);
    collection.drop();
  }

  private static MongoCollection getCollection(Jongo jongo) {
    return jongo.getCollection(COLLECTION_NAME);
  }

  @SneakyThrows
  private static Jongo createJongo(String mongoUri) {
    return new Jongo(getDb(mongoUri));
  }

  private static DB getDb(String uri) throws UnknownHostException {
    val mongoUri = new MongoClientURI(uri);
    val mongo = new MongoClient(mongoUri);

    return mongo.getDB(mongoUri.getDatabase());
  }

}
