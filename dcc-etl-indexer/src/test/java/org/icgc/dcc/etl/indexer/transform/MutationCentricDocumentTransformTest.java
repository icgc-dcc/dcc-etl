package org.icgc.dcc.etl.indexer.transform;

import static org.icgc.dcc.etl.indexer.model.DocumentType.MUTATION_CENTRIC_TYPE;

import java.util.concurrent.ExecutionException;

import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.json.JSONException;
import org.junit.Test;

public class MutationCentricDocumentTransformTest extends AbstractDocumentTransformTest {

  @Test
  public void testCreate() throws InterruptedException, ExecutionException, JSONException {
    compare();
  }

  @Override
  protected DocumentType getType() {
    return MUTATION_CENTRIC_TYPE;
  }

}
