package org.icgc.dcc.etl.indexer.service;

import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;
import lombok.Cleanup;
import lombok.val;

import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class IndexServiceTest {

  @Test
  public void testMappingWellformedness() {
    @Cleanup
    val service = new IndexService(newTransportClient("es://localhost:9300"));
    val types = ImmutableList.copyOf(DocumentType.values());

    service.initializeIndex("test-index", types);
  }

}
