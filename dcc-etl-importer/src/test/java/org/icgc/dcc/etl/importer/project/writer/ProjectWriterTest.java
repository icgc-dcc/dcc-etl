package org.icgc.dcc.etl.importer.project.writer;

import static org.icgc.dcc.etl.importer.util.Importers.getLocalMongoClientUri;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.common.test.mongodb.EmbeddedMongo;
import org.icgc.dcc.etl.importer.project.model.Project;
import org.jongo.Jongo;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientURI;

@Slf4j
@Ignore
public class ProjectWriterTest {

  /**
   * Constants.
   */
  private static final String MONGO_DB_NAME = "project-importer-test";
  private static final String MONGO_RELEASE_PROJECT_DB_NAME = MONGO_DB_NAME + "."
      + ReleaseCollection.PROJECT_COLLECTION.getId();

  @Rule
  public final static EmbeddedMongo embeddedMongo = new EmbeddedMongo();

  @Test
  @SneakyThrows
  public void testWrite() {
    val mongoClientUri = getLocalMongoClientUri(embeddedMongo.getPort(), MONGO_RELEASE_PROJECT_DB_NAME);
    @Cleanup
    val writer = new ProjectWriter(mongoClientUri);

    writer.write(ImmutableList.of(Project.builder().__project_id("test").build()));

    log.info("Project: {}", getJongo(mongoClientUri).getCollection(ReleaseCollection.PROJECT_COLLECTION.getId())
        .findOne()
        .as(JsonNode.class));
  }

  private static Jongo getJongo(MongoClientURI mongoClientUri) {
    val mongo = embeddedMongo.getMongo();
    val db = mongo.getDB(mongoClientUri.getDatabase());

    return new Jongo(db);
  }

}
