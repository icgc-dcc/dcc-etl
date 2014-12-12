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
package org.icgc.dcc.etl.importer.go.writer;

import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.importer.geneset.model.GeneSetType.GO_TERM;
import static org.icgc.dcc.etl.importer.go.util.GoAssociationIndexer.indexUniProtId;
import static org.icgc.dcc.etl.importer.util.Genes.getGeneId;

import java.util.Set;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.gene.writer.AbstractGeneGeneSetWriter;
import org.icgc.dcc.etl.importer.go.model.GoModel;
import org.icgc.dcc.etl.importer.go.util.GoGeneGeneSetsBuilder;
import org.jongo.MongoCollection;

@Slf4j
public class GoGeneGeneSetWriter extends AbstractGeneGeneSetWriter {

  public GoGeneGeneSetWriter(@NonNull MongoCollection geneCollection) {
    super(geneCollection, GO_TERM);
  }

  public void write(@NonNull GoModel model) {
    log.info("Clearing gene GO gene sets...");
    clearGeneGeneSets();

    log.info("Creating gene GO gene sets builder...");
    val geneSetsBuilder = new GoGeneGeneSetsBuilder(model);

    log.info("Updating gene gene sets...");
    val watch = createStarted();
    int geneCount = 0;
    for (val gene : getGeneWithUniprots()) {
      val geneId = getGeneId(gene);

      log.debug("Building gene sets for gene: {}", geneId);
      val geneSets = geneSetsBuilder.build(gene);
      log.debug("Built {} gene sets for gene {}: {}", formatCount(geneSets), geneId, geneSets);

      // Update
      addGeneSets(gene, geneSets);

      // Save
      saveGene(gene);

      if (++geneCount % 1000 == 0) {
        log.info("Updated {} {} genes", formatCount(geneCount), watch);
      }
    }

    // RQ6
    val uniprotIdAssociationsIndex = indexUniProtId(model.getAssociations());
    reportUniprotIds(uniprotIdAssociationsIndex.keySet());
  }

  private void reportUniprotIds(Set<String> associatedUniprotIds) {
    log.info("There are {} distinct uniprot ids that exist in the associations", formatCount(associatedUniprotIds));

    val existingUniprotIds = getDistinctGeneUniprotIds();
    log.info("There are {} distinct uniprot ids that exist the gene model", formatCount(existingUniprotIds));

    val existingOnly = difference(existingUniprotIds, associatedUniprotIds);
    log.info("There are {} distinct uniprot ids that exist only in the gene model", formatCount(existingOnly));

    val associatedOnly = difference(associatedUniprotIds, existingUniprotIds);
    log.info("There are {} distinct uniprot ids that exist only in the associations", formatCount(associatedOnly));

    val common = intersection(associatedUniprotIds, existingUniprotIds);
    log.info("There are {} distinct uniprot ids that are common to both", formatCount(common));
  }

}
