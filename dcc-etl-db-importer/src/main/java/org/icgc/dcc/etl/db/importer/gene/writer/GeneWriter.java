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
package org.icgc.dcc.etl.db.importer.gene.writer;

import static com.google.common.base.Stopwatch.createStarted;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.db.importer.gene.util.AllGeneFilter.all;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.gene.core.GeneCallback;
import org.icgc.dcc.etl.db.importer.gene.core.GeneConverter;
import org.icgc.dcc.etl.db.importer.gene.core.GeneFilter;
import org.icgc.dcc.etl.db.importer.util.AbstractJongoWriter;
import org.jongo.MongoCollection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.mongodb.MongoClientURI;

import de.undercouch.bson4jackson.BsonFactory;

/**
 * Imports from Heliotrope {@code genes.bson} {@code mongodump} file into DCC gene database.
 */
@Slf4j
public class GeneWriter extends AbstractJongoWriter<InputStream> {

  /**
   * How frequently to report status on inserted genes.
   */
  private static final int STATUS_GENE_COUNT = 10000;

  public GeneWriter(MongoClientURI mongoUri) {
    super(mongoUri);
  }

  @Override
  public void write(@NonNull InputStream inputStream) {
    write(inputStream, all());
  }

  @SneakyThrows
  public void write(@NonNull InputStream geneStream, @NonNull GeneFilter geneFilter) {
    val watch = createStarted();
    log.info("Writing gene model into {}...", mongoUri);

    // Open BSON file stream
    val genes = readGenes(geneStream);

    writeGenes(watch, genes, geneFilter);
  }

  private void writeGenes(Stopwatch watch, MappingIterator<JsonNode> genes, GeneFilter geneFilter)
      throws UnknownHostException, IOException {
    val genesCollection = getCollection(GENE_COLLECTION);

    // Drop the current collection
    log.info("Dropping current gene collection, if any...");
    clearGenes(genesCollection);

    log.info("Saving gene documents");
    saveGenes(genesCollection, genes, geneFilter);

    log.info("Wrote gene model in {}", watch);
  }

  private void clearGenes(MongoCollection genesCollection) {
    genesCollection.drop();
  }

  private void saveGenes(final MongoCollection genesCollection, MappingIterator<JsonNode> genes, GeneFilter geneFilter)
      throws IOException {
    // Transform and save

    val geneConverter = new GeneConverter();
    processGenes(genes, geneFilter, new GeneCallback() {

      @Override
      public void handle(JsonNode gene) {
        val convertedGene = geneConverter.convert(gene);

        genesCollection.save(convertedGene);
      }

    });
  }

  private MappingIterator<JsonNode> readGenes(InputStream geneStrem) throws IOException, JsonProcessingException {
    val mapper = new ObjectMapper(new BsonFactory());

    return mapper.reader(ObjectNode.class).readValues(geneStrem);
  }

  private void processGenes(MappingIterator<JsonNode> genes, GeneFilter filter, GeneCallback callback)
      throws IOException {
    try {
      int insertCount = 0;
      int excludeCount = 0;
      val watch = createStarted();

      while (genes.hasNextValue()) {
        val gene = genes.next();

        val include = filter.filter(gene);
        if (!include) {
          excludeCount++;
          continue;
        }

        // Delegate
        callback.handle(gene);

        if (++insertCount % STATUS_GENE_COUNT == 0) {
          log.info("Saved {} gene documents ({} documents/s)",
              formatCount(insertCount), formatCount(STATUS_GENE_COUNT / (watch.elapsed(SECONDS))));
          watch.reset().start();
        }
      }
      log.info("Finished loading {} gene(s) total, excluded {} genes total",
          formatCount(insertCount), formatCount(excludeCount));
    } finally {
      genes.close();
    }
  }

}
