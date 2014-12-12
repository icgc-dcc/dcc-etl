package org.icgc.dcc.etl.indexer.io;

import static org.icgc.dcc.etl.indexer.factory.JongoFactory.newJongo;
import static org.icgc.dcc.etl.indexer.model.CollectionFields.DEFAULT_COLLECTION_FIELDS;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("Useful for debugging only")
public class DirectMemoryMongoDBCollectionReaderTest {

  @Test
  @SneakyThrows
  @SuppressWarnings("deprecation")
  public void testRead() {
    @Cleanup
    val reader = new DirectMemoryMongoDBCollectionReader(newJongo("mongodb://localhost/etl-test-release17-0-0"));
    for (@SuppressWarnings("unused")
    val value : reader.readObservations(DEFAULT_COLLECTION_FIELDS)) {
      // No-op
    }
  }

}
