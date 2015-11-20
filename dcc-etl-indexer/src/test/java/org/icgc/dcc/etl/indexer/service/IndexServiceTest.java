package org.icgc.dcc.etl.indexer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;

import java.util.List;

import lombok.Cleanup;
import lombok.val;

import org.icgc.dcc.common.core.model.IndexType;
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

  @Test
  public void testMappingAvailability() {
    val documentTypeCount = getDocumentTypes();
    val indexTypeCount = getIndexTypes();
    assertThat(indexTypeCount).containsOnlyElementsOf(documentTypeCount);
  }

  private static List<String> getDocumentTypes() {
    return stream(DocumentType.values())
        .map(dt -> dt.getName())
        .collect(toImmutableList());
  }

  private static List<String> getIndexTypes() {
    return stream(IndexType.values())
        .map(dt -> dt.getName())
        .collect(toImmutableList());
  }

}
