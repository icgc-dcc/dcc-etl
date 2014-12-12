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
package org.icgc.dcc.etl.loader.mongodb;

import static com.google.common.base.Joiner.on;
import static lombok.AccessLevel.PRIVATE;

import java.util.List;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Indexes a mongodb collection.
 */
@Slf4j
@NoArgsConstructor(access = PRIVATE)
final class MongoIndexer {

  private static final String INDEX_NAME_SEPARATOR = "_";

  private static final String INDEX_NAME_SUFFIX = "idx";

  static void ensureUniqueIndex(DBCollection dbCollection, List<String> keys, String... desc) {
    ensureIndex(dbCollection, keys, true, desc);
  }

  static void ensureNonUniqueIndex(DBCollection dbCollection, List<String> keys, String... desc) {
    ensureIndex(dbCollection, keys, false, desc);
  }

  private static void ensureIndex(DBCollection dbCollection, List<String> keys, boolean unique, String... desc) {
    String databaseName = dbCollection.getDB().getName();
    String collectionName = dbCollection.getName();
    DBObject businessKeySelector = jsonSelector(keys);
    String indexName = indexName(collectionName, desc);

    log.info("Ensuring " + (unique ? "" : "non-") + "unique index '{}' exists for '{}' in '{}.{}'",
        new Object[] { indexName, businessKeySelector, databaseName, collectionName });
    dbCollection.ensureIndex(businessKeySelector, indexName, unique);
  }

  private static String indexName(String collectionName, String... desc) {
    return on(INDEX_NAME_SEPARATOR)
        .join(
            collectionName,
            on(INDEX_NAME_SEPARATOR)
                .join(desc),
            INDEX_NAME_SUFFIX);
  }

  private static DBObject jsonSelector(List<String> fieldNames) {
    BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
    for (String fieldName : fieldNames) {
      builder.add(fieldName, 1);
    }
    return builder.get();
  }

}
