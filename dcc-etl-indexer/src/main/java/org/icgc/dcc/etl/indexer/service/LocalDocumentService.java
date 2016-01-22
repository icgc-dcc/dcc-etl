/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.icgc.dcc.etl.indexer.factory.JongoFactory.newJongo;
import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import lombok.NonNull;

import org.elasticsearch.client.Client;
import org.icgc.dcc.etl.indexer.core.Config;
import org.icgc.dcc.etl.indexer.core.DocumentTask;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.icgc.dcc.etl.indexer.task.LocalDocumentTask;
import org.jongo.Jongo;

/**
 * High level service facade for local node index creation.
 */
public class LocalDocumentService extends AbstractDocumentService {

  /**
   * Input client.
   */
  private final Jongo inputClient;

  /**
   * Output client.
   */
  private final Client outputClient;

  public LocalDocumentService(@NonNull Config config) {
    super(config);
    this.inputClient = newJongo(config.getMongoUri());
    this.outputClient = newTransportClient(config.getEsUri(), false);
  }

  @Override
  protected DocumentTask createTask(@NonNull DocumentType type) {
    return new LocalDocumentTask(type, config, inputClient, outputClient);
  }

  @Override
  protected ExecutorService createExecutor() {
    return sameThreadExecutor();
  }

  @Override
  public void close() throws IOException {
    inputClient.getDatabase().getMongo().close();
    outputClient.close();
  }

}
