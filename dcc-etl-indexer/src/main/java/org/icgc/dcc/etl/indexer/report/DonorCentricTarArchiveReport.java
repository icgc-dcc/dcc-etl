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
package org.icgc.dcc.etl.indexer.report;

import static com.google.common.collect.Maps.newHashMap;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getDonorGenes;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getGeneGeneSets;
import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_CENTRIC_TYPE;

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.common.hadoop.fs.FileSystems;
import org.icgc.dcc.etl.indexer.core.Document;
import org.icgc.dcc.etl.indexer.io.TarArchiveEntryCallback;
import org.icgc.dcc.etl.indexer.io.TarArchivesProcessor;

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
public class DonorCentricTarArchiveReport {

  /**
   * Dependencies.
   */
  @NonNull
  private final TarArchivesProcessor processor;

  public void report(@NonNull FileSystem fileSystem, @NonNull String archiveDir, @NonNull String indexName) {
    val watch = Stopwatch.createStarted();
    log.info("Processing '{}'...", DONOR_CENTRIC_TYPE);
    val processor = new TarArchivesProcessor(fileSystem, archiveDir, indexName);

    val reportCallback = new ReportDocumentCallback();
    processor.process(DONOR_CENTRIC_TYPE, reportCallback);

    log.info("Finished processing in {} - {}", watch, reportCallback.getReport());
  }

  @SneakyThrows
  public static void main(String[] args) {
    val options = new Options();
    new JCommander(options).parse(args);
    log.info("Running with: {}", options);

    // Config
    val archiveDir = options.archiveDir;
    val indexName = options.indexName;
    val fileSystem = FileSystems.getFileSystem(options.hadoop);

    val report = new DonorCentricTarArchiveReport(new TarArchivesProcessor(fileSystem, archiveDir, indexName));
    report.report(fileSystem, archiveDir, indexName);
  }

  private static class ReportDocumentCallback implements TarArchiveEntryCallback {

    /**
     * Aggregrations
     */
    long totalDonorCount = 0;
    long totalDonorGeneCount = 0;
    long totalDonorGeneSetCount = 0;

    SummaryStatistics stats = new SummaryStatistics();

    @Override
    public void onSettings(ObjectNode settings) {
      // No-op
    }

    @Override
    public void onMapping(String mappingTypeName, ObjectNode mapping) {
      // No-op
    }

    @Override
    public void onDocument(Document document) {
      val donor = document.getSource();
      long donorCount = 1;

      // Genes
      val donorGenes = getDonorGenes(donor);
      int donorGeneCount = donorGenes.size();

      // GeneSets
      long donorGeneSetCount = 0;
      for (val donorGene : donorGenes) {
        val donorGeneSets = getGeneGeneSets(donorGene);

        donorGeneSetCount += donorGeneSets.size();
      }

      // Aggregate
      totalDonorCount += donorCount;
      totalDonorGeneCount += donorGeneCount;
      totalDonorGeneSetCount += donorGeneSetCount;
      stats.addValue(donorGeneSetCount);

      if (totalDonorCount % 1000 == 0) {
        log.info(getReport());
      }
    }

    public String getReport() {
      return String
          .format(
              "totalDonorCount = %s, totalDonorGeneCount = %s, totalDonorGeneSetCount = %s, averageDonorGeneSetCount = %s, stats = %s",
              formatCount(totalDonorCount),
              formatCount(totalDonorGeneCount),
              formatCount(totalDonorGeneSetCount),
              formatCount(totalDonorGeneSetCount / totalDonorCount),
              stats);
    }

  }

  public static class Options {

    /**
     * Config - Index
     */
    @Parameter(names = { "--index-name" }, description = "The name of the index to create (e.g. ICGC15)")
    public String indexName = "test17-indexer-donor-centric";

    /**
     * Input - Archive
     */
    @Parameter(names = { "--archive-dir" }, description = "Input local or HDFS archive and VCF directory (e.g. /tmp, /icgc/releases)")
    public String archiveDir = "/***REMOVED***";

    /**
     * Hadoop
     */
    @DynamicParameter(names = "-D", description = "Hadoop properties")
    public Map<String, String> hadoop = newHashMap();

  }

}
