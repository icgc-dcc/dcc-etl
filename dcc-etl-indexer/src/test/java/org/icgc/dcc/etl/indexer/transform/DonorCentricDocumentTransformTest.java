package org.icgc.dcc.etl.indexer.transform;

import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_CENTRIC_TYPE;

import java.util.concurrent.ExecutionException;

import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.json.JSONException;
import org.junit.Test;

public class DonorCentricDocumentTransformTest extends AbstractDocumentTransformTest {

  @Test
  public void testCreate() throws InterruptedException, ExecutionException, JSONException {
    compare();
  }

  @Override
  protected DocumentType getType() {
    return DONOR_CENTRIC_TYPE;
  }

}
