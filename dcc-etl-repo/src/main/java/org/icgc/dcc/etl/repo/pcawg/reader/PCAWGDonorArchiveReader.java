package org.icgc.dcc.etl.repo.pcawg.reader;

import static com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@Slf4j
@RequiredArgsConstructor
public class PCAWGDonorArchiveReader {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper().configure(AUTO_CLOSE_SOURCE, false);
  private static final ObjectReader READER = MAPPER.reader(ObjectNode.class);

  /**
   * State.
   */
  @NonNull
  private final URL donorArchiveUrl;

  public Iterable<ObjectNode> readDonors() throws IOException {
    log.info("Reading donors from '{}'...", donorArchiveUrl);

    @Cleanup
    val iterator = readValues();
    return ImmutableList.copyOf(iterator);
  }

  private MappingIterator<ObjectNode> readValues() throws IOException {
    return READER.readValues(openStream());
  }

  @SneakyThrows
  private InputStream openStream() {
    return donorArchiveUrl.openStream();
  }

}