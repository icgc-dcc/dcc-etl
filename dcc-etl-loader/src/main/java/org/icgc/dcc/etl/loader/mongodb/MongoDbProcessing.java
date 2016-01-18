/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.icgc.dcc.common.core.model.FieldNames.MONGO_INTERNAL_ID;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DONOR_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.MUTATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import static org.icgc.dcc.common.core.util.Joiners.DOT;
import static org.icgc.dcc.etl.core.util.MongoDbUtils.fields;

import java.util.List;
import java.util.Set;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.bson.types.ObjectId;
import org.icgc.dcc.common.core.model.BusinessKeys;
import org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames;
import org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * Pre- and post-processes the mongodb collections.
 * <p>
 * Pre-processing indexes collections and post-processing helps with detecting and removing potential duplicates as
 * described in https://jira.oicr.on.ca/browse/DCC-1448 (the latter to be removed soon).
 */
@Slf4j
public class MongoDbProcessing {

  /**
   * Returns the field names for a mutation.
   * <p>
   * Careful to always keep this in sync with {@link LoaderMongoDbScheme#mutationFields()}.
   */
  public static final List<String> MUTATION_FIELDS = ImmutableList.<String> builder()
      .addAll(BusinessKeys.MUTATION)
      .add(IdentifierFieldNames.SURROGATE_MUTATION_ID)
      .build();

  /**
   * Creates indices for the mongodb collections.
   */
  public static void index(MongoClientURI releaseMongoClientURI, String databaseName) {
    Jongo jongo = jongo(releaseMongoClientURI, databaseName);

    MongoIndexer.ensureUniqueIndex(
        getDBCollection(jongo, DONOR_COLLECTION),
        DONOR_COLLECTION.getPrimaryKey(),
        "pk");
    MongoIndexer.ensureUniqueIndex(
        getDBCollection(jongo, MUTATION_COLLECTION),
        MUTATION_COLLECTION.getPrimaryKey(),
        "pk");
    MongoIndexer.ensureUniqueIndex(
        getDBCollection(jongo, MUTATION_COLLECTION),
        BusinessKeys.MUTATION_IDENTIFYING_PART,
        "id");
    MongoIndexer.ensureUniqueIndex(
        getDBCollection(jongo, OBSERVATION_COLLECTION),
        OBSERVATION_COLLECTION.getPrimaryKey(),
        "pk");
    MongoIndexer.ensureNonUniqueIndex(
        getDBCollection(jongo, OBSERVATION_COLLECTION),
        newArrayList(IdentifierFieldNames.SURROGATE_DONOR_ID),
        "fk");
    MongoIndexer.ensureNonUniqueIndex(
        getDBCollection(jongo, OBSERVATION_COLLECTION),
        newArrayList(IdentifierFieldNames.SURROGATE_MUTATION_ID),
        "mut");
    MongoIndexer.ensureNonUniqueIndex(
        getDBCollection(jongo, OBSERVATION_COLLECTION),
        newArrayList(DOT.join(LoaderFieldNames.CONSEQUENCE_ARRAY_NAME, LoaderFieldNames.GENE_ID)),
        "gene");
    MongoIndexer.ensureUniqueIndex(
        getDBCollection(jongo, MUTATION_COLLECTION),
        MUTATION_FIELDS,
        "all");
  }

  /**
   * See {@link MongoDbProcessing} comment about post-processing.
   */
  public static void postProcessMongoDb(MongoClientURI releaseMongoClientURI, String databaseName) {
    Jongo jongo = jongo(releaseMongoClientURI, databaseName);

    MongoCollection donorCollection = jongo.getCollection(DONOR_COLLECTION.getId());
    MongoCollection mutationCollection = jongo.getCollection(MUTATION_COLLECTION.getId());
    MongoCollection observationCollection = jongo.getCollection(OBSERVATION_COLLECTION.getId());

    // Check observations
    checkState(
        // A limit of 1 is sufficient for error detection
        getOccurencesWithNoObservations(observationCollection, 1) == 0,
        "All occurences should have observations: '%s' don't",
        getOccurencesWithNoObservations(observationCollection, 0));

    // Process the 'Donor' collection
    removeDuplicates(
        donorCollection,
        detectDuplicates(
            donorCollection,
            DONOR_COLLECTION.getPrimaryKey()));

    // Process the 'Mutation' collection
    removeDuplicates(
        mutationCollection,
        detectDuplicates(
            mutationCollection,
            MUTATION_COLLECTION.getPrimaryKey()));

    // Process the 'Observation' collection
    removeDuplicates(
        observationCollection,
        detectDuplicates(
            observationCollection,
            OBSERVATION_COLLECTION.getPrimaryKey()));

  }

  private static int getOccurencesWithNoObservations(MongoCollection observationCollection, int limit) {
    return observationCollection
        .find("{'" + LoaderFieldNames.OBSERVATION_ARRAY_NAME + "': {$size: 0}}")
        .limit(limit)
        .as(ObjectNode.class)
        .count();
  }

  /**
   * Detects duplicates mentioned at {@link MongoDbProcessing}.
   */
  private static Set<ObjectId> detectDuplicates(MongoCollection collection, List<String> primaryKeyFields) {
    log.info("Detecting duplicates in {} for fields {}", collection.getName(), primaryKeyFields);

    Set<ObjectId> duplicates = newHashSet();
    String selector = fields(primaryKeyFields);

    ObjectNode previousPrimaryKey = null;
    for (JsonNode document : collection
        .find()
        .projection(selector)
        .limit(0) // Unlimited sort (see http://docs.mongodb.org/manual/reference/method/cursor.limit/#cursor.limit)
        .sort(selector) // Expects unique index on the fields
        .as(ObjectNode.class)) {

      // Remove internal mongodb ID
      checkState(document.isObject(),
          "'%s' is expected to be an object, '%s' instead",
          document, document.getClass().getSimpleName());
      ObjectNode primaryKey = (ObjectNode) document;
      String objectIdString = // The removed node is returned
          primaryKey.
              remove(MONGO_INTERNAL_ID) // Always present
              .asText();

      if (isEquivalent(
          primaryKey, // ObjectId was removed at the step just above
          previousPrimaryKey)) {
        log.error("Duplicate found for '{}' (ObjectId is '{}')", primaryKey, objectIdString);
        duplicates.add(new ObjectId(objectIdString));
      }

      previousPrimaryKey = primaryKey;
    }

    return duplicates;
  }

  /**
   * Fixes duplicates mentioned at {@link MongoDbProcessing}.
   */
  private static void removeDuplicates(MongoCollection collection, Set<ObjectId> duplicates) {
    if (duplicates.isEmpty()) {
      log.info("OK - No duplicates found for collection {}", collection.getName());
    } else {
      log.warn("Removing {} duplicates in {}", duplicates.size(), collection.getName());
    }

    for (ObjectId duplicate : duplicates) {
      collection.remove(duplicate);
    }
  }

  /**
   * Determines whether the two {@link ObjectNode} are equivalent of one another.
   * <p>
   * This uses full (deep) equality according to the jackson doc. It is safe for the RHS to be null.
   */
  private static boolean isEquivalent(ObjectNode primaryKey, JsonNode previous) {
    return primaryKey.equals(previous);
  }

  private static DBCollection getDBCollection(Jongo jongo, ReleaseCollection releaseCollection) {
    return jongo.getCollection(releaseCollection.getId()).getDBCollection();
  }

  @SneakyThrows
  private static Jongo jongo(MongoClientURI releaseMongoClientURI, String databaseName) {
    return new Jongo(
        new MongoClient(releaseMongoClientURI)
            .getDB(databaseName));
  }

}
