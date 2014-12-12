package org.icgc.dcc.etl.indexer.core;

import static org.icgc.dcc.etl.indexer.factory.JongoFactory.newJongo;
import lombok.val;

import org.icgc.dcc.etl.indexer.io.MongoDBCollectionReader;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Useful for debugging only")
public class DocumentProcessorTest {

  @Test
  public void testProcess() {
    val reader = createReader();
    val processor = new DocumentProcessor("document-processor", DocumentType.MUTATION_CENTRIC_TYPE, reader);
    processor.process();
  }

  private CollectionReader createReader() {
    return new MongoDBCollectionReader(
        newJongo("mongodb://" + System.getProperty("etl") + "@***REMOVED***/test17-0-31"));
  }

}
