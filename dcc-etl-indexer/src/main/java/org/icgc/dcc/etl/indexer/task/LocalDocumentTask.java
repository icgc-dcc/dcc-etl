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
package org.icgc.dcc.etl.indexer.task;

import java.io.File;
import java.io.IOException;

import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.elasticsearch.client.Client;
import org.icgc.dcc.etl.indexer.core.CollectionReader;
import org.icgc.dcc.etl.indexer.core.Config;
import org.icgc.dcc.etl.indexer.core.DocumentWriter;
import org.icgc.dcc.etl.indexer.io.ElasticSearchDocumentWriter;
import org.icgc.dcc.etl.indexer.io.MongoDBCollectionReader;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.icgc.dcc.etl.indexer.schema.SchemaWriter;
import org.jongo.Jongo;

import com.google.common.collect.ImmutableList;

public class LocalDocumentTask extends AbstractDocumentTask {

  private final Jongo inputClient;
  private final Client outputClient;

  public LocalDocumentTask(@NonNull DocumentType type, @NonNull Config config,
      @NonNull Jongo inputClient, @NonNull Client outputClient) {
    super(type, config);
    this.inputClient = inputClient;
    this.outputClient = outputClient;
  }

  @Override
  protected CollectionReader createCollectionReader() {
    return new MongoDBCollectionReader(inputClient) {

      @Override
      public void close() throws IOException {
        // No-op since we are using a shared client
      }

    };
  }

  @Override
  protected Iterable<DocumentWriter> createWriters(DocumentType type, FileSystem fileSystem) throws IOException {
    // Extend parents writers
    val writers = ImmutableList.<DocumentWriter> builder();
    writers.addAll(super.createWriters(type, fileSystem));
    writers.add(createSchemaWriter(type));

    return writers.build();
  }

  @Override
  protected DocumentWriter createElasticSearchWriter(DocumentType type) {
    return new ElasticSearchDocumentWriter(outputClient, config.getIndexName(), type, 10, false);
  }

  private SchemaWriter createSchemaWriter(DocumentType type) {
    return new SchemaWriter(new File("target", type.getName() + ".schema"));
  }

}
