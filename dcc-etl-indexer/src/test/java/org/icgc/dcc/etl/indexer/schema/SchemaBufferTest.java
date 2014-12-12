package org.icgc.dcc.etl.indexer.schema;

import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;

import org.icgc.dcc.etl.indexer.schema.SchemaBuffer;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SchemaBufferTest {

  static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testGetSchemaWithPOJONodes() {
    val buffer = new SchemaBuffer();

    // Setup
    val document = MAPPER.createObjectNode();
    document.putPOJO("list", ImmutableList.of(1, 2, 3));
    document.putPOJO("map", ImmutableMap.of("x", 1, "y", 3));
    document.putPOJO("number", 1);
    document.putPOJO("string", "x");

    // Exercise
    buffer.addDocument(document);

    // Verify
    val schema = buffer.getSchema().iterator();
    assertThat(schema.next()).isEqualTo("list.$: NUMBER");
    assertThat(schema.next()).isEqualTo("list: ARRAY");
    assertThat(schema.next()).isEqualTo("map.x: NUMBER");
    assertThat(schema.next()).isEqualTo("map.y: NUMBER");
    assertThat(schema.next()).isEqualTo("number: NUMBER");
    assertThat(schema.next()).isEqualTo("string: STRING");
  }

}
