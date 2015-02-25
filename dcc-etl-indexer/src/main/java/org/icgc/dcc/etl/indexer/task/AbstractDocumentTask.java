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
package org.icgc.dcc.etl.indexer.task;

import static com.google.common.io.Closeables.close;

import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.exists;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.rm;
import static org.icgc.dcc.etl.indexer.factory.JongoFactory.newJongo;
import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;
import static org.icgc.dcc.etl.indexer.model.DocumentType.MUTATION_CENTRIC_TYPE;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.zip.GZIPOutputStream;

import lombok.Cleanup;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.etl.indexer.core.CollectionReader;
import org.icgc.dcc.etl.indexer.core.Config;
import org.icgc.dcc.etl.indexer.core.DocumentProcessor;
import org.icgc.dcc.etl.indexer.core.DocumentTask;
import org.icgc.dcc.etl.indexer.core.DocumentWriter;
import org.icgc.dcc.etl.indexer.io.ElasticSearchDocumentWriter;
import org.icgc.dcc.etl.indexer.io.MutationVCFDocumentWriter;
import org.icgc.dcc.etl.indexer.io.TarArchiveDocumentWriter;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.icgc.dcc.etl.indexer.repository.ProjectRepository;
import org.icgc.dcc.etl.indexer.util.DocumentWriterCallback;

import cascading.flow.Flow;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

/**
 * Unit of concurrency that executes cluster-side within a flow executor slot.
 */
@Slf4j
@EqualsAndHashCode
@RequiredArgsConstructor
public abstract class AbstractDocumentTask implements DocumentTask {

  /**
   * See
   * https://wiki.oicr.on.ca/display/DCCSOFT/Aggregated+Data+Download+Specification?focusedCommentId=57774680#comment
   * -57774680
   */
  private static final String VCF_FILE_NAME = "simple_somatic_mutation.aggregated.vcf.gz";

  /**
   * Execution parameters.
   */
  @NonNull
  protected final DocumentType type;
  @NonNull
  protected final Config config;

  @Override
  @SneakyThrows
  public Flow<?> call() {
    log.info(Strings.repeat("-", 80));
    log.info("Executing type {} '{}'", this.getClass().getSimpleName(), type);
    log.info(Strings.repeat("-", 80));
    execute();

    return null;
  }

  @Override
  public void execute() throws Exception {
    // Create resources
    val fileSystem = FileSystem.get(new URI(config.getFsUri()), new Configuration());
    val writers = createWriters(type, fileSystem);
    val reader = createCollectionReader();
    val processor = createProcessor(type, reader, writers);

    try {
      processor.process();
    } finally {
      // Cleanup
      val swallow = true;
      for (val writer : writers) {
        close(writer, swallow);
      }

      close(reader, swallow);
    }
  }

  /**
   * Template method.
   * 
   * @return the collection reader.
   */
  abstract CollectionReader createCollectionReader();

  protected Iterable<DocumentWriter> createWriters(DocumentType type, FileSystem fileSystem) throws IOException {
    val writers = ImmutableList.<DocumentWriter> builder();

    writers.add(createTarArchiveWriter(fileSystem));
    writers.add(createElasticSearchWriter(type));
    if (isVCFExportable(type)) {
      // Special case export
      writers.add(createVCFWriter(fileSystem));
    }

    return writers.build();
  }

  protected DocumentWriter createTarArchiveWriter(FileSystem fileSystem) throws IOException {
    // Config
    val bufferSize = 8 * 1024;

    val archiveName = String.format("%s-%s.tar.gz", config.getIndexName(), type.getName());
    val archivePath = new Path(config.getOutputDir(), archiveName);
    if (exists(fileSystem, archivePath)) {
      log.info("Removing archive '{}'...", archivePath);
      rm(fileSystem, archivePath);
    }

    val archiveOutputStream =
        new GZIPOutputStream(new BufferedOutputStream(fileSystem.create(archivePath), bufferSize));

    log.info("Creating tar archive writer for archive file '{}'...", archivePath);
    return new TarArchiveDocumentWriter(config.getIndexName(), archiveOutputStream);
  }

  protected DocumentWriter createVCFWriter(FileSystem fileSystem) throws IOException {
    // Config
    val bufferSize = 8 * 1024;

    int totalSsmTestedDonorCount = getTotalSsmTestedDonorCount();

    val vcfPath = new Path(config.getOutputDir(), VCF_FILE_NAME);
    if (exists(fileSystem, vcfPath)) {
      log.info("Removing VCF file '{}'...", vcfPath);
      rm(fileSystem, vcfPath);
    }

    val vcfOutputStream =
        new GZIPOutputStream(new BufferedOutputStream(fileSystem.create(vcfPath), bufferSize));

    log.info("Creating VCF writer for path '{}'...", vcfPath);
    return new MutationVCFDocumentWriter(config.getReleaseName(), config.getIndexName(), config.getFastaFile(),
        vcfOutputStream, totalSsmTestedDonorCount);
  }

  protected DocumentWriter createElasticSearchWriter(DocumentType type) {
    // Sets the number of concurrent requests allowed to be executed. A value of 0 means that only a single request will
    // be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed while accumulating
    // new bulk requests.
    val concurrentRequests = 1; // 2014-08-26: Changed from 10 to 1 to reduce chance for OOM
    return new ElasticSearchDocumentWriter(newTransportClient(config.getEsUri()), config.getIndexName(), type,
        concurrentRequests);
  }

  protected DocumentProcessor createProcessor(DocumentType type, CollectionReader reader,
      Iterable<DocumentWriter> writers) {
    val processor = new DocumentProcessor(config.getIndexName(), type, reader);

    for (val writer : writers) {
      processor.addCallback(new DocumentWriterCallback(writer));
    }

    return processor;
  }

  private boolean isVCFExportable(DocumentType type) {
    return config.isExportVCF() && type == MUTATION_CENTRIC_TYPE;
  }

  private int getTotalSsmTestedDonorCount() throws IOException {
    @Cleanup
    ProjectRepository repository = new ProjectRepository(newJongo(config.getMongoUri()));

    return repository.getTotalSsmTestedDonorCount();
  }

}
