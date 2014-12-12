package org.icgc.dcc.etl.indexer.transform;

import static org.icgc.dcc.etl.indexer.model.DocumentType.MUTATION_CENTRIC_TYPE;
import static org.icgc.dcc.common.test.json.JsonNodes.$;

import java.io.File;

import lombok.val;

import org.icgc.dcc.etl.indexer.core.Document;
import org.icgc.dcc.etl.indexer.transform.MutationCentricSummaryCallback;
import org.icgc.dcc.common.test.json.JsonNodes;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class MutationCentricSummaryCallbackTest {

  @Test
  public void testCall() throws JsonProcessingException {
    val source = (ObjectNode) $(new File("src/test/resources/fixtures/summary/MutationDocument.json"));
    val document = new Document(MUTATION_CENTRIC_TYPE, "MU37643", source);

    val callback = new MutationCentricSummaryCallback();
    callback.call(document);

    val json = JsonNodes.MAPPER.writeValueAsString(document.getSource());

    System.out.println(json);
  }

}
