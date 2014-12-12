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
package org.icgc.dcc.etl.stats;

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DONOR_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;

import java.net.UnknownHostException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * Gathers statistics about MongoDB entities.
 */
@Slf4j
@RequiredArgsConstructor
public class MongoStats {

  @NonNull
  private final String uri;
  @NonNull
  private final String databaseName;

  void stats(StatsResults results) {
    log.info("Using uri: {}", uri);
    val jongo = createJongo(databaseName);

    val donorCollection = jongo.getCollection(DONOR_COLLECTION.getId());
    results.mongoDonorCount = donorCollection.count();

    val observationCollection = jongo.getCollection(OBSERVATION_COLLECTION.getId());
    results.mongoObservationDonorCount = uniqueCount(observationCollection);
    results.mongoObservationCount = observationCollection.count();

    val mutationCollection = jongo.getCollection(ReleaseCollection.MUTATION_COLLECTION.getId());
    results.mongoMutationCount = mutationCollection.count();
  }

  /**
   * TODO: not very efficient...
   */
  private long uniqueCount(MongoCollection observationCollection) {
    val iterator = observationCollection
        .distinct(DONOR_ID)
        .as(String.class)
        .iterator();

    long count = 0;
    while (iterator.hasNext()) {
      count++;
      iterator.next();
    }

    return count;
  }

  @SneakyThrows
  private Jongo createJongo(String releaseName) {
    String mongoUri = mongoUri(releaseName);

    log.info("Using connection: {}", mongoUri);
    DB db = db(mongoUri);

    return new Jongo(db);
  }

  private String mongoUri(String releaseName) {
    return uri + (uri.charAt(uri.length() - 1) == '/' ? "" : "/") + releaseName;
  }

  private DB db(String releaseUri) throws UnknownHostException {
    val uri = new MongoClientURI(releaseUri);
    val mongo = new MongoClient(uri);

    return mongo.getDB(uri.getDatabase());
  }
}
