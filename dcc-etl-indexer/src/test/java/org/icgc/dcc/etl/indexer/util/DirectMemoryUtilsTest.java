package org.icgc.dcc.etl.indexer.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.FormatUtils.formatBytes;
import static org.icgc.dcc.etl.indexer.util.DirectMemoryUtils.getDirectMemorySize;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.junit.Test;

@Slf4j
public class DirectMemoryUtilsTest {

  @Test
  public void testGet() throws Exception {
    val value = getDirectMemorySize();
    log.info("getDirectMemorySize: {}", formatBytes(value));

    assertThat(value).isGreaterThan(0L);
  }

}
