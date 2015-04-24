package org.icgc.dcc.etl.db.importer.pcawg.reader;

import static com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.util.AbstractTsvMapReader;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

@Slf4j
@RequiredArgsConstructor
public class PCAWGReader extends AbstractTsvMapReader {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper().configure(AUTO_CLOSE_SOURCE, false);

  /**
   * State.
   */
  @NonNull
  private final URL donorArchiveUrl;

  public Iterable<ObjectNode> read() throws IOException {
    log.info("Reading donors from '{}'...", donorArchiveUrl);
    val reader = MAPPER.reader(ObjectNode.class);

    @Cleanup
    MappingIterator<ObjectNode> iterator = reader.readValues(openStream());

    val donors = Lists.<ObjectNode> newArrayList();
    while (iterator.hasNext()) {
      val donor = iterator.next();
      donors.add(donor);
    }

    return donors;
  }

  @SneakyThrows
  private InputStream openStream() throws IOException, MalformedURLException {
    return new GZIPInputStream(donorArchiveUrl.openStream());
  }

}