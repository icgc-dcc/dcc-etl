package org.icgc.dcc.etl.repo.pcawg.reader;

import static com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE;
import static org.icgc.dcc.common.core.util.URLs.getUrl;
import static org.icgc.dcc.etl.repo.pcawg.util.PCAWGArchives.PCAWG_ARCHIVE_BASE_URL;

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
   * JSONL (new line delimited JSON file) dump of clean, curated PCAWG donor information.
   * 
   * @see http://jsonlines.org/
   */
  public static final URL DEFAULT_PCAWG_DONOR_ARCHIVE_URL =
      getUrl(PCAWG_ARCHIVE_BASE_URL + "/gnos_metadata/latest/reports/sanger_variant_called_donors.jsonl");
  public static final URL ALTERNATE_PCAWG_DONOR_ARCHIVE_URL =
      getUrl(PCAWG_ARCHIVE_BASE_URL + "/gnos_metadata/latest/reports/donors_with_bwa_alignment.jsonl");

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

  public PCAWGDonorArchiveReader() {
    this.donorArchiveUrl = DEFAULT_PCAWG_DONOR_ARCHIVE_URL;
  }

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