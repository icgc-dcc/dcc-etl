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
package org.icgc.dcc.etl.indexer.schema;

import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.io.Files.createParentDirs;
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getFileSystem;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.indexer.cli.DocumentTypeConverter;
import org.icgc.dcc.etl.indexer.core.Document;
import org.icgc.dcc.etl.indexer.io.TarArchiveEntryCallback;
import org.icgc.dcc.etl.indexer.io.TarArchivesProcessor;
import org.icgc.dcc.etl.indexer.model.DocumentType;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;

/**
 * Reverse engineers the schema from Elasticsearch.
 */
@Slf4j
@RequiredArgsConstructor
public class TarArchiveReverseSchemaGenerator {

  public static class Options {

    /**
     * Config - Index
     */
    @Parameter(names = { "--index-name" }, description = "The name of the index to create (e.g. ICGC17)")
    public String indexName = "test17-indexer-donor-centric";

    /**
     * Input - Archive
     */
    @Parameter(names = { "--archive-dir" }, description = "Input local or HDFS archive and VCF directory (e.g. /tmp)")
    public String archiveDir = "/tmp";

    /**
     * Output - Schema
     */
    @Parameter(names = { "--schema-dir" }, description = "Output local schema directory")
    public File schemaDir = new File("target");

    /**
     * Behavior
     */
    @Parameter(names = { "--types" }, converter = DocumentTypeConverter.class, description = "Document types to create. Comma seperated list of: 'donor', 'donor-centric', 'gene-centric', 'gene-project', 'project', 'observation-centric', 'mutation-centric'. By default all index types will be created.")
    public List<DocumentType> types = newArrayList(DocumentType.values());

    /**
     * Hadoop
     */
    @DynamicParameter(names = "-D", description = "Hadoop properties")
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

  @SneakyThrows
  public void generate(DocumentType type, final SchemaWriter writer) {
    processor.process(type, new TarArchiveEntryCallback() {

      @Override
      public void onSettings(ObjectNode settings) {
        // No-op
      }

      @Override
      public void onMapping(String mappingTypeName, ObjectNode mapping) {
        // No-op
      }

      @Override
      @SneakyThrows
      public void onDocument(Document document) {
        writer.write(document);
      }

    });
  }

  private static File prepareSchemaFile(File schemaDir, DocumentType type) throws IOException {
    val schemaFile = new File(schemaDir, type.getName() + ".schema");
    schemaFile.delete();
    if (!schemaDir.exists()) {
      createParentDirs(schemaDir);
    }

    return schemaFile;
  }

  /**
   * Utility to reverse engineer all index type schemata from Elasticsearch.
   */
  @SneakyThrows
  public static void main(String[] args) {
    val options = new Options();
    new JCommander(options).parse(args);
    log.info("Running with: {}", options);

    // Config
    val schemaDir = options.schemaDir;
    val archiveDir = options.archiveDir;
    val indexName = options.indexName;
    val fileSystem = getFileSystem(options.hadoop);

    val watch = Stopwatch.createStarted();

    val generator = new TarArchiveReverseSchemaGenerator(new TarArchivesProcessor(fileSystem, archiveDir, indexName));
    for (val type : DocumentType.values()) {
      log.info("{}", repeat("-", 80));
      log.info("Processing '{}' document type...", type);
      log.info("{}", repeat("-", 80));

      val schemaFile = prepareSchemaFile(schemaDir, type);

      @Cleanup
      val writer = new SchemaWriter(schemaFile);

      generator.generate(type, writer);
    }

    log.info("Finished processing in {}", watch);
  }

}
