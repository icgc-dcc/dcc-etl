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
package org.icgc.dcc.etl.importer.util;

import static org.icgc.dcc.etl.importer.util.ValuesWrapper.ABSENT_VALUES_WRAPPER;

import java.net.UnknownHostException;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.jongo.Jongo;
import org.jongo.MongoCollection;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

@Slf4j
@Data
@AllArgsConstructor
public class CollectionImporter {

  @NonNull
  private final MongoClientURI sourceMongoUri;
  @NonNull
  private final MongoClientURI targetMongoUri;

  /**
   * Values to include in the import. If absent, all values are included except if they are explicitly excluded (see
   * {@link #exclusionValues}).
   */
  @NonNull
  private final Optional<ValuesWrapper> inclusionValues;

  /**
   * Values to exclude in the import. If absent, all values are included. {@link #inclusionValues} trumps this
   * mechanism.
   */
  @NonNull
  private final Optional<ValuesWrapper> exclusionValues;

  public CollectionImporter(@NonNull MongoClientURI sourceMongoUri,
      @NonNull MongoClientURI targetMongoUri) {
    this.sourceMongoUri = sourceMongoUri;
    this.targetMongoUri = targetMongoUri;

    this.inclusionValues = ABSENT_VALUES_WRAPPER;
    this.exclusionValues = ABSENT_VALUES_WRAPPER;
  }

  public void import_(String collectionName) {
    log.info("Importing {} from {} into {} in {}", collectionName, sourceMongoUri, collectionName,
        targetMongoUri);

    log.info("  Reading from: {}", sourceMongoUri);
    Jongo sourceJongo = getSourceJongo();

    log.info("  Writing to:   {}", targetMongoUri);
    Jongo targetJongo = getTargetJongo();

    if (inclusionValues.isPresent()) {
      log.info("Explicitely including: {}", inclusionValues.get());
    } else {
      log.info("No inclusion values provided");
    }

    if (exclusionValues.isPresent()) {
      log.info("Explicitely excluding: {}", exclusionValues.get());
    } else {
      log.info("No exclusion values provided");
    }

    MongoCollection sourceCollection = sourceJongo.getCollection(collectionName);
    MongoCollection targetCollection = targetJongo.getCollection(collectionName);

    // Ensure idempotence
    clear(targetCollection);

    execute(sourceCollection, targetCollection);
  }

  private void clear(MongoCollection targetCollection) {
    log.info("Clearing target collection {}...", targetCollection);
    val result = targetCollection.remove();
    log.info("Finished clearing target collection {}: {}", targetCollection, result);
  }

  private void execute(MongoCollection sourceCollection, MongoCollection targetCollection) {
    Iterable<JsonNode> sourceDocuments = sourceCollection.find().as(JsonNode.class);
    for (JsonNode sourceDocument : sourceDocuments) {
      if ((!inclusionValues.isPresent() && !exclusionValues.isPresent()) ||
          (inclusionValues.isPresent() && inclusionValues.get().isMatch(sourceDocument)) || // Trumps the exclusion list
          (exclusionValues.isPresent() && !exclusionValues.get().isMatch(sourceDocument))) {
        targetCollection.save(sourceDocument);
      }
    }
  }

  private Jongo getSourceJongo() {
    return createJongo(sourceMongoUri);
  }

  private Jongo getTargetJongo() {
    return createJongo(targetMongoUri);
  }

  @SneakyThrows
  private Jongo createJongo(MongoClientURI mongoUri) {
    DB db = db(mongoUri);

    return new Jongo(db);
  }

  private DB db(MongoClientURI mongoUri) throws UnknownHostException {
    Mongo mongo = new MongoClient(mongoUri);

    return mongo.getDB(mongoUri.getDatabase());
  }

}
