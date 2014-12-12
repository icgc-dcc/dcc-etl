/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.etl.loader.core;

import static org.apache.commons.lang.StringUtils.repeat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategy;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.stats.FlowStats;

import com.google.common.base.Stopwatch;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * {@code LoaderJob} is the top level {@code Runnable} responsible for executing it a {@link LoaderPlan}.
 */
@Slf4j
@RequiredArgsConstructor
public class LoaderJob implements Runnable {

  @NonNull
  private final LoaderContext context;
  @NonNull
  private final LoaderPlatformStrategy platformStrategy;

  @NonNull
  private final LoaderPlan plan;

  @Override
  public void run() {
    val jobTimer = Stopwatch.createStarted();

    // Initialise the file system (emptying WD mostly)
    platformStrategy.initialise(plan.getDataDigest());

    // Run cascades
    val cascade = plan.getCascade();
    log.info(banner());
    log.info("Running cascade '{}'", cascade.getName());
    log.info(banner());

    val cascadeTimer = Stopwatch.createStarted();
    runCascade(cascade);

    log.info(banner());
    log.info("");
    log.info("FINISHED CASCADE: '{}' in {}", cascade.getName(), cascadeTimer);
    log.info("");
    log.info(banner());

    // Finalise the file system (cleaning up working directories)
    platformStrategy.finalise(plan.getDataDigest());

    log.info("Finished job in {}", jobTimer);
  }

  @SneakyThrows
  public void logCollectionStats(MongoClientURI mongoUri) {
    @Cleanup
    Mongo mongo = new MongoClient(mongoUri);
    DB db = mongo.getDB(mongoUri.getDatabase());

    for (String collectionName : db.getCollectionNames()) {
      DBCollection collection = db.getCollection(collectionName);
      CommandResult stats = collection.getStats();
      log.info("Collection '{}' stats: \n  {}", collectionName, stats);
    }
  }

  /**
   * Runs a cascade corresponding to a submission.
   */
  private void runCascade(Cascade cascade) {
    final String counterGroup = cascade.getName();
    for (Flow<?> flow : cascade.getFlows()) {
      flow.addListener(new ConsoleCounterFlowListener(counterGroup));

      flow.writeDOT(LoaderPlan.dotFile("loader-flow-%s.dot", flow.getName()));
    }

    cascade.writeDOT(LoaderPlan.dotFile("loader-cascade.dot"));

    ExecutorService s = monitorCascade(cascade, counterGroup);

    // Blocks until finished
    cascade.complete();
    s.shutdown();
  }

  /**
   * Monitors the cascade on a periodic basis.
   */
  private ExecutorService monitorCascade(final Cascade cascade, final String counterGroup) {
    ScheduledExecutorService s = Executors.newScheduledThreadPool(1);
    s.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        for (Flow<?> flow : cascade.getFlows()) {
          FlowStats stats = flow.getStats();
          if (stats.isRunning()) {
            for (String counter : stats.getCountersFor(counterGroup)) {
              log.info("{}:{}", counter, stats.getCounterValue(counterGroup, counter));
            }
          }
        }
      }

    }, 10, 10, TimeUnit.SECONDS);

    return s;
  }

  /**
   * {@code FlowListener} implementation that prints flow counters to the console.
   */
  private static final class ConsoleCounterFlowListener implements FlowListener {

    /**
     * The counter group to print.
     */
    private final String counterGroup;

    /**
     * Creates a new flow listener
     * 
     * @param counterGroup
     */
    private ConsoleCounterFlowListener(String counterGroup) {
      this.counterGroup = counterGroup;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void onStarting(Flow flow) {
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void onStopping(Flow flow) {
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void onCompleted(Flow flow) {
      for (String counter : flow.getStats().getCountersFor(counterGroup)) {
        log.info("{}:{}", counter, flow.getStats().getCounterValue(counterGroup, counter));
      }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean onThrowable(Flow flow, Throwable throwable) {
      return false;
    }
  }

  private static String banner() {
    return repeat("-", 80);
  }

}
