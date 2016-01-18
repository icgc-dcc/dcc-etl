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
package org.icgc.dcc.etl.indexer.io;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getFileSystem;
import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;

import java.util.List;
import java.util.Map;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.client.Client;
import org.icgc.dcc.etl.indexer.cli.DocumentTypeConverter;
import org.icgc.dcc.etl.indexer.core.Document;
import org.icgc.dcc.etl.indexer.core.DocumentService;
import org.icgc.dcc.etl.indexer.model.DocumentType;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;

/**
 * An index importer that can ingest the current {@link DocumentService} produced JSON tarball archives (stored in HDFS
 * in production, but can also load from local) into ES.
 * <p>
 * We can do this using the new ES Java API from Java 7 after the “document construction" process is complete. As a
 * result we make a clean break from the CDH constraint (which is holding us back because it requires Java 6). More
 * importantly, we are free to upgrade Elasticsearch when the Portal API has upgraded to the latest API.
 */
@Slf4j
@RequiredArgsConstructor
public class TarArchiveIndexImporter {

  public static class Options {

    /**
     * Config - Index
     */
    @Parameter(names = { "--index-name" }, description = "The name of the index to create (e.g. ICGCxy)")
    public String indexName = "etl-test-release17-0-0";

    /**
     * Input - Archive
     */
    @Parameter(names = { "--archive-dir" }, description = "Input local or HDFS archive directory (e.g. /tmp, /icgc/dcc/overarch/jobs/ICGC/xy/0/1)")
    public String archiveDir = "src/test/resources/fixtures/archive-dir";

    /**
     * Output - Index
     */
    @Parameter(names = { "--es-uri" }, description = "Output Elasticsearch base URI (e.g. es://localhost:9300)")
    public String esUri = "es://localhost:9300";

    /**
     * Behavior
     */
    @Parameter(names = { "--types" }, converter = DocumentTypeConverter.class, description = "Document types to import. Comma seperated list of: 'donor', 'donor-centric', 'gene-centric', 'gene-project', 'project', 'observation-centric', 'mutation-centric'. By default all index types will be created. (e.g. project,mutation-centric)")
    public List<DocumentType> types = newArrayList(DocumentType.values());

    /**
     * Hadoop
     */
    @DynamicParameter(names = "-D", description = "Hadoop properties. (e.g -Dfs.defaultFS=hdfs://localhost, -Dmapred.job.tracker=localhost)")
    public Map<String, String> hadoop = newHashMap();

    /**
     * Info
     */
    @Parameter(names = { "-v", "--version" }, help = true, description = "Show version information")
    public boolean version;
    @Parameter(names = { "-h", "--help" }, help = true, description = "Show help information")
    public boolean help;

  }

  /**
   * Dependencies.
   */
  @NonNull
  private final TarArchivesProcessor processor;
  @NonNull
  private final Client client;

  public void execute(String indexName) {
    val watch = Stopwatch.createStarted();
    for (val type : DocumentType.values()) {
      log.info("{}", repeat("-", 80));
      log.info("Importing '{}' document type...", type);
      log.info("{}", repeat("-", 80));

      execute(indexName, type);
    }

    log.info("Finished importing in {}", watch);
  }

  @SneakyThrows
  public void execute(final String indexName, final DocumentType type) {
    val indexClient = client.admin().indices();

    @Cleanup
    val writer = new ElasticSearchDocumentWriter(client, indexName, type, 10, true);

    log.info("Checking index '{}' for existence...", indexName);
    val exists = indexClient.prepareExists(indexName)
        .execute()
        .actionGet()
        .isExists();

    processor.process(type, new TarArchiveEntryCallback() {

      @Override
      public void onSettings(ObjectNode settings) {
        // Only do this the first time
        if (!exists) {
          log.info("Creating index '{}'...", indexName);
          checkState(indexClient
              .prepareCreate(indexName)
              .setSettings(settings.toString())
              .execute()
              .actionGet()
              .isAcknowledged(),
              "Index '%s' creation was not acknowledged!", indexName);
        }
      }

      @Override
      public void onMapping(String mappingTypeName, ObjectNode mapping) {
        log.info("Creating mapping for type '{}'...", indexName, mappingTypeName);
        checkState(indexClient.preparePutMapping(indexName)
            .setType(mappingTypeName)
            .setSource(mapping.toString())
            .execute()
            .actionGet()
            .isAcknowledged(),
            "Index '%s' type mapping in index '%s' was not acknowledged for release '%s'!",
            mappingTypeName, indexName);
      }

      @Override
      @SneakyThrows
      public void onDocument(Document document) {
        writer.write(document);
      }

    });
  }

  /**
   * Utility to reverse engineer all index type schemata from Elasticsearch.
   */
  @SneakyThrows
  public static void main(String[] args) {
    // Parse
    val options = parseOptions(args);
    log.info("Running with: {}", options);

    // Config
    val esUri = options.esUri;
    val archiveDir = options.archiveDir;
    val indexName = options.indexName;
    val fileSystem = getFileSystem(options.hadoop);

    // Dependencies
    @Cleanup
    val client = newTransportClient(esUri);
    val processor = new TarArchivesProcessor(fileSystem, archiveDir, indexName);

    // Construct
    val importer = new TarArchiveIndexImporter(processor, client);

    // Import
    val watch = createStarted();
    importer.execute(indexName);
    log.info("Finished importing '{}' in {}!", indexName, watch);
  }

  private static Options parseOptions(String[] args) {
    val options = new Options();
    new JCommander(options).parse(args);

    return options;
  }

}
