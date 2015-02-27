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
package org.icgc.dcc.etl.db.importer.gene.writer;

import static org.icgc.dcc.common.core.model.FieldNames.GENE_SETS_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_UNIPROT_IDS;

import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.etl.db.importer.gene.model.GeneGeneSet;
import org.icgc.dcc.etl.db.importer.geneset.model.GeneSetType;
import org.icgc.dcc.etl.db.importer.util.LongBatchQueryModifier;
import org.jongo.MongoCollection;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;

@RequiredArgsConstructor
public abstract class AbstractGeneGeneSetWriter {

  /**
   * Dependencies.
   */
  @NonNull
  protected final MongoCollection geneCollection;
  @NonNull
  protected final GeneSetType type;

  protected int clearGeneGeneSets() {
    val value = type.toString().toLowerCase();
    return geneCollection
        .update("{}")
        .multi()
        .with("{ $pull: { " + type.getFieldName() + ": { " + GENE_SETS_TYPE + ": # } } }", value)
        .getN();
  }

  protected Iterable<ObjectNode> getGenes() {
    return geneCollection
        .find()
        .with(new LongBatchQueryModifier())
        .as(ObjectNode.class);
  }

  protected Iterable<ObjectNode> getGeneWithUniprots() {
    return geneCollection
        .find("{ " + GENE_UNIPROT_IDS + ": { $ne : null } }") // Has uniprots
        .with(new LongBatchQueryModifier())
        .as(ObjectNode.class);
  }

  protected Set<String> getDistinctGeneUniprotIds() {
    return ImmutableSet.copyOf(
        geneCollection
            .distinct(GENE_UNIPROT_IDS)
            .query("{ " + GENE_UNIPROT_IDS + ": { $ne : null } }") // Avoid nulls
            .as(String.class));
  }

  protected void saveGene(ObjectNode gene) {
    geneCollection.save(gene);
  }

  protected void addGeneSets(ObjectNode gene, Set<GeneGeneSet> geneSets) {
    val setsNode = gene.withArray(type.getFieldName());
    for (val geneSet : geneSets) {
      setsNode.addPOJO(geneSet);
    }
  }

}
