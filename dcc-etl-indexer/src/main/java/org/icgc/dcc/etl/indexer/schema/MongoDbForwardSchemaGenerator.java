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
package org.icgc.dcc.etl.indexer.schema;

import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.etl.indexer.factory.JongoFactory.newJongo;

import java.io.File;
import java.io.IOException;
import java.util.List;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.indexer.cli.DirectoryValidator;
import org.icgc.dcc.etl.indexer.cli.DocumentTypeConverter;
import org.icgc.dcc.etl.indexer.core.CollectionReader;
import org.icgc.dcc.etl.indexer.core.DocumentProcessor;
import org.icgc.dcc.etl.indexer.core.DocumentWriter;
import org.icgc.dcc.etl.indexer.io.MongoDBCollectionReader;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.icgc.dcc.etl.indexer.util.DocumentWriterCallback;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;

/**
 * Forward engineers the schema from MongoDB.
 */
@Slf4j
@ToString
public class MongoDbForwardSchemaGenerator {

  @Parameter(names = { "-m", "--mongo-uri" }, required = true, description = "The MongoDB URI (e.g. mongodb://localhost:27017/<release-name>")
  private String mongoUri;

  @Parameter(names = { "-o", "--output-dir" }, required = true, validateValueWith = DirectoryValidator.class, description = "Output directory (e.g. ./target)")
  private File outputDir;

  @Parameter(names = { "-t", "--types" }, converter = DocumentTypeConverter.class, description = "Document types for which to create schemata. Comma seperated list of: 'donor', 'donor-centric', 'gene-centric', 'gene-project', 'project', 'observation-centric', 'mutation-centric'. By default all index types will be created.")
  private List<DocumentType> types = newArrayList(DocumentType.values());

  @Parameter(names = { "-l", "--limit" }, description = "The maximum number of documents to inference")
  private int limit = Integer.MAX_VALUE;

  /**
   * Utility to forward engineer all index type schemata from MongoDB.
   * @throws IOException
   */
  public static void main(String... args) throws IOException {
    new MongoDbForwardSchemaGenerator().run(args);
  }

  public void run(String... args) throws IOException {
    new JCommander(this).parse(args);
    log.info("Running with: {}", this);

    generate();
  }

  private void generate() throws IOException {
    val watch = Stopwatch.createStarted();
    for (val type : types) {
      log.info("{}", repeat("-", 80));
      log.info("Processing '{}' document type...", type);
      log.info("{}", repeat("-", 80));

      val schemaFile = createSchemaFile(type);

      @Cleanup
      SchemaWriter writer = new SchemaWriter(schemaFile);

      generate(type, writer);
    }

    log.info("Finished processing in {}", watch);
  }

  @SneakyThrows
  public void generate(DocumentType type, SchemaWriter writer) {
    // Setup
    @Cleanup
    val reader = new MongoDBCollectionReader(newJongo(mongoUri));
    val processor = createProcessor(type, writer, reader);

    // Generate
    processor.process();
  }

  private File createSchemaFile(DocumentType type) throws IOException {
    val schemaFile = new File(outputDir, type.getName() + ".schema");
    schemaFile.delete();

    return schemaFile;
  }

  private DocumentProcessor createProcessor(DocumentType type, DocumentWriter writer, CollectionReader reader) {
    DocumentProcessor processor = new DocumentProcessor("", type, reader) {

      @Override
      protected Iterable<ObjectNode> readCollection() {
        // Limit the number of returned results
        return limit(super.readCollection(), limit);
      }

    };

    processor.addCallback(new DocumentWriterCallback(writer));

    return processor;
  }

}
