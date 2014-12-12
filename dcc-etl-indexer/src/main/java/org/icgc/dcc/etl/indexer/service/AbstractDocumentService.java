/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.indexer.service;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.size;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;

import java.io.IOException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.indexer.core.Config;
import org.icgc.dcc.etl.indexer.core.DocumentService;
import org.icgc.dcc.etl.indexer.core.DocumentTask;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.icgc.dcc.etl.indexer.util.Stopwatches;

import cascading.flow.Flow;
import cascading.stats.FlowStats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractDocumentService implements DocumentService {

  /**
   * Configuration.
   */
  @NonNull
  protected Config config;

  /**
   * Dependencies.
   */
  private IndexService indexService;

  @Override
  public void writeDocuments() throws IOException {
    writeDocuments(DocumentType.values());
  }

  @Override
  public void writeDocuments(DocumentType... types) throws IOException {
    writeDocuments(ImmutableList.copyOf(types));
  }

  @Override
  public void writeDocuments(Iterable<DocumentType> types) throws IOException {
    String indexName = config.getIndexName();

    // Prepare
    log.info("Initializing index...");
    getIndexService().initializeIndex(indexName);

    // Populate
    log.info("Populating index...");
    write(types);

    // Report
    log.info("Reporting index...");
    getIndexService().reportIndex(indexName);

    // Compact
    log.info("Optimizing index...");
    getIndexService().optimizeIndex(indexName);

    // Freeze
    log.info("Freezing index...");
    getIndexService().freezeIndex(indexName);
  }

  protected void write(Iterable<DocumentType> types) throws IOException {
    // Create tasks
    int i = 1;
    int n = size(types);
    val tasks = Lists.<DocumentTask> newArrayList();
    for (val type : types) {
      // Create task
      log.info("{}", repeat("-", 90));
      log.info("[{}/{}] Creating '{}' processor task", new Object[] { i++, n, type.getName() });
      log.info("{}", repeat("-", 90));
      val task = createTask(type);

      tasks.add(task);
    }

    // Execute tasks
    try {
      executeTasks(tasks);
    } catch (Throwable t) {
      propagate(t);
    }
  }

  /**
   * Template method for implementations to create tasks.
   */
  protected abstract DocumentTask createTask(DocumentType type);

  /**
   * Template method for implementations to create executor for executing tasks.
   */
  protected abstract ExecutorService createExecutor();

  protected void executeTasks(Iterable<DocumentTask> tasks) throws InterruptedException {
    // Delegate to implementations
    val executor = createExecutor();
    val service = new ExecutorCompletionService<Flow<?>>(executor);

    try {
      val count = size(tasks);
      log.info("Starting {} documents tasks...", count);
      val watch = Stopwatches.createStarted();

      for (val task : tasks) {
        log.info("Submitting task '{}'...", task);
        service.submit(task);
      }

      // But allow tasks to finish
      executor.shutdown();

      // This will iterate in completion order, failing fast if there is a task failure
      for (int i = 0; i < count; i++) {
        val flow = service.take().get();

        reportStats(flow);
      }

      log.info("Finished executing {} document tasks in {}!", count, watch);
    } catch (Throwable t) {
      log.error("Aborting task executions due to exception...");
      propagate(t);
    }
  }

  protected void reportStats(Flow<?> flow) {
    if (flow != null) {
      log.info("Flow '{}' stats: {}", flow.getID(), formatFlowStats(flow.getFlowStats()));
    }
  }

  private static String formatFlowStats(FlowStats flowStats) {
    // TODO: Format
    return flowStats.toString();
  }

  private IndexService getIndexService() {
    if (indexService == null) {
      indexService = new IndexService(newTransportClient(config.getEsUri()));
    }

    return indexService;
  }

}
