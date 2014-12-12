package org.icgc.dcc.etl.indexer.service;

import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;
import lombok.Cleanup;
import lombok.val;

import org.icgc.dcc.etl.indexer.service.IndexService;
import org.junit.Test;

public class IndexServiceTest {

  @Test
  public void testMappingWellformedness() {
    @Cleanup
    val service = new IndexService(newTransportClient("es://localhost:9300"));

    service.initializeIndex("test-index");
  }

}
