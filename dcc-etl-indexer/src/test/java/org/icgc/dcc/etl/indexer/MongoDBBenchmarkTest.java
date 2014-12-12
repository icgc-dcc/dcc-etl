package org.icgc.dcc.etl.indexer;

import static com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.icgc.dcc.common.core.util.FormatUtils.formatBytes;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatRate;
import static org.icgc.dcc.etl.indexer.factory.JongoFactory.newJongo;

import java.io.IOException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.QueryModifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.mongodb.DBCursor;

@Slf4j
public class MongoDBBenchmarkTest {

  /**
   * Test input.
   */
  static final String TEST_MONGO_URI = "mongodb://localhost/dcc-release-load-prod-07e-42-ICGC15-feb07_2";

  Jongo jongo;

  @Before
  public void setUp() {
    jongo = newJongo(TEST_MONGO_URI);
  }

  @After
  public void tearDown() {
    jongo.getDatabase().getMongo().close();
  }

  @Test
  public void testSerial() throws IOException {
    int count = 0;
    val watch = Stopwatch.createStarted();
    for (val releaseCollection : ReleaseCollection.values()) {
      processCollection(jongo, releaseCollection);

      count++;
    }

    log.info("Finished reading {} collections in {}", count, watch);
  }

  @Test
  public void testParallel() throws IOException, InterruptedException {
    int count = 0;
    val watch = Stopwatch.createStarted();

    val executor = newFixedThreadPool(ReleaseCollection.values().length);
    for (val releaseCollection : ReleaseCollection.values()) {
      executor.execute(new Runnable() {

        @Override
        public void run() {
          processCollection(jongo, releaseCollection);
        }
      });

      count++;
    }

    executor.shutdown();
    executor.awaitTermination(1, HOURS);
    log.info("Finished reading {} collections in {}", count, watch);
  }

  private void processCollection(Jongo jongo, ReleaseCollection releaseCollection) {
    int count = 0;
    val watch = Stopwatch.createStarted();
    val collection = jongo.getCollection(releaseCollection.getId());

    for (val value : find(collection)) {
      value.size();

      if (count % 1000 == 0) {
        log.info(
            "Processed {} '{}' documents ({} docs/s) in {} ({})",
            new Object[] { formatCount(count), releaseCollection, formatRate(rate(watch, count)), watch, formatFreeMemory() });
      }

      count++;
    }

    log.info("Finished processing {} '{}' in {} ({})",
        new Object[] { formatCount(count), releaseCollection, watch, formatFreeMemory() });
  }

  private static Iterable<ObjectNode> find(MongoCollection collection) {
    return collection.find().with(new QueryModifier() {

      @Override
      public void modify(DBCursor cursor) {
        // Prevent time outs due to idle cursors after an inactivity period (10 minutes)
        cursor.setOptions(QUERYOPTION_NOTIMEOUT);
      }

    }).as(ObjectNode.class);
  }

  private static float rate(Stopwatch watch, int count) {
    val seconds = watch.elapsed(MILLISECONDS) / 1000.0f;
    if (seconds == 0.0f) {
      return 0.0f;
    }

    return count / seconds;
  }

  private static String formatFreeMemory() {
    return formatBytes(Runtime.getRuntime().freeMemory());
  }

}
