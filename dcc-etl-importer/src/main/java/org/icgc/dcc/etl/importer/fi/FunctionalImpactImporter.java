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
package org.icgc.dcc.etl.importer.fi;

import static com.google.common.base.Joiner.on;
import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCE_TYPES;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.Joiners.DOT;
import static org.icgc.dcc.etl.importer.fi.core.FunctionalImpactCalculator.calculateImpact;

import java.net.UnknownHostException;
import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.fi.CompositeImpactCategory;
import org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames;
import org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames;
import org.icgc.dcc.etl.importer.util.LongBatchQueryModifier;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * Runs the basic algorithm as outlined in: https://wiki.oicr.on.ca/display/DCCSOFT/SSM+functional+impact+annotation
 */
@Slf4j
@RequiredArgsConstructor
public class FunctionalImpactImporter {

  @NonNull
  private final MongoClientURI mongoUri;

  @SneakyThrows
  public void execute() {
    val watch = Stopwatch.createStarted();
    val jongo = createJongo();
    val observationCollection = jongo.getCollection(OBSERVATION_COLLECTION.getId());
    val observations = readObservations(observationCollection);

    val db = jongo.getDatabase();
    val oldCollection = db.getCollection(OBSERVATION_COLLECTION.getId());
    val existingIndices = oldCollection.getIndexInfo();
    log.info("Existing indices on DB:");
    for (DBObject o : existingIndices) {
      log.info("{}", o.toString());
    }

    val tempCollectionName = OBSERVATION_COLLECTION.getId() + "_temp";
    db.getCollection(tempCollectionName).drop();
    val tempMongoCollection = db.createCollection(tempCollectionName, new BasicDBObject("autoIndexId", false));
    val tempJongoCollection = jongo.getCollection(tempCollectionName);

    writeObservations(jongo, tempJongoCollection, observationCollection, observations);
    observations.close();

    log.info("Finished writing observations in {} ...", watch);
    watch.reset().start();

    tempMongoCollection.rename(OBSERVATION_COLLECTION.getId(), true);
    val newObservationCollection = db.getCollection(OBSERVATION_COLLECTION.getId());

    // turn on the index on _id
    ensureUniqueIndex(
        newObservationCollection,
        newArrayList("_id"),
        "id");

    // Create the rest of the indices, see org.icgc.dcc.etl.loader.mongodb.MongoDbProcessing.index
    ensureUniqueIndex(
        newObservationCollection,
        OBSERVATION_COLLECTION.getPrimaryKey(),
        "pk");
    ensureNonUniqueIndex(
        newObservationCollection,
        newArrayList(IdentifierFieldNames.SURROGATE_DONOR_ID),
        "fk");
    ensureNonUniqueIndex(
        newObservationCollection,
        newArrayList(IdentifierFieldNames.SURROGATE_MUTATION_ID),
        "mut");
    ensureNonUniqueIndex(
        newObservationCollection,
        newArrayList(DOT.join(LoaderFieldNames.CONSEQUENCE_ARRAY_NAME, LoaderFieldNames.GENE_ID)),
        "gene");

    val newIndices = newObservationCollection.getIndexInfo();
    log.info("Newly created indices on DB:");
    for (DBObject o : newIndices) {
      log.info("{}", o.toString());
    }

    log.info("Finished switching collection in {} ...", watch);

  }

  private void writeObservations(Jongo jongo, MongoCollection tempCollection, MongoCollection observationCollection,
      MongoCursor<ObjectNode> observations) {
    long observationCounter = 0;
    long consequenceCounter = 0;
    val modifiedObservations = Lists.<ObjectNode> newArrayList();

    log.info("Calculating functional impact predictions");
    for (val observation : observations) {
      observationCounter++;
      val consequences = (ArrayNode) observation.get(LoaderFieldNames.CONSEQUENCE_ARRAY_NAME);
      val impactSummary = Sets.<String> newHashSet();

      if (null != consequences && consequences.isArray() && consequences.size() > 0) {
        for (val consequence : consequences) {
          consequenceCounter++;
          val fiNode = consequence.get(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION);

          val functionalImpactPredictionSummary =
              calculateImpact(consequence.get(OBSERVATION_CONSEQUENCE_TYPES).asText(), fiNode);

          impactSummary.add(functionalImpactPredictionSummary);
          ((ObjectNode) consequence).put(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY,
              functionalImpactPredictionSummary);
        }
      } else {
        impactSummary.add(CompositeImpactCategory.UNKNOWN.name());
      }

      if (impactSummary.size() > 0) {
        val summaryNode = JsonNodeFactory.instance.arrayNode();
        for (val item : impactSummary) {
          summaryNode.add(item.toString());
        }

        observation.put(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY, summaryNode);
        modifiedObservations.add(observation);
      }

      if (observationCounter % 50000 == 0) {
        insert(tempCollection, modifiedObservations);
        modifiedObservations.clear();
        log.info("Processed {} observations, {} consequences", formatCount(observationCounter),
            formatCount(consequenceCounter));
      }
    }

    insert(tempCollection, modifiedObservations);
    log.info("Processed {} observations, {} consequences", formatCount(observationCounter),
        formatCount(consequenceCounter));
  }

  private MongoCursor<ObjectNode> readObservations(MongoCollection observationCollection) {

    return observationCollection
        .find()
        .with(new LongBatchQueryModifier())
        .as(ObjectNode.class);
  }

  private Jongo createJongo() throws UnknownHostException {
    val database = mongoUri.getDatabase();
    val mongo = new MongoClient(mongoUri);
    val mongoDB = mongo.getDB(database);

    return new Jongo(mongoDB);
  }

  private void insert(MongoCollection observationCollection, List<ObjectNode> observations) {
    val watch = Stopwatch.createStarted();
    observationCollection.insert(observations.toArray());
    log.info("Inserted {} observations in {} ", formatCount(observations.size()), watch.stop());
  }

  /*
   * This section is taken almost verbatim from MongoIndexer in package org.icgc.dcc.etl.loader.mongodb.
   * 
   * //TODO factor MongoIndexer to etl core to make it reusable across projects.
   */

  private static final String INDEX_NAME_SEPARATOR = "_";

  private static final String INDEX_NAME_SUFFIX = "idx";

  private static void ensureUniqueIndex(DBCollection dbCollection, List<String> keys, String... desc) {
    ensureIndex(dbCollection, keys, true, desc);
  }

  private static void ensureNonUniqueIndex(DBCollection dbCollection, List<String> keys, String... desc) {
    val watch = Stopwatch.createStarted();
    ensureIndex(dbCollection, keys, false, desc);
    log.info("Finished indexing in {} ...", watch);
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
