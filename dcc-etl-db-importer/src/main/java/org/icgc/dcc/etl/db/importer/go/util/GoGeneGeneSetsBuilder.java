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
package org.icgc.dcc.etl.db.importer.go.util;

import static org.icgc.dcc.etl.db.importer.geneset.model.GeneSetAnnotation.DIRECT;
import static org.icgc.dcc.etl.db.importer.geneset.model.GeneSetAnnotation.INFERRED;
import static org.icgc.dcc.etl.db.importer.geneset.model.GeneSetType.GO_TERM;
import static org.icgc.dcc.etl.db.importer.go.util.GoAssociationIndexer.indexGoId;
import static org.icgc.dcc.etl.db.importer.go.util.GoAssociationIndexer.indexUniProtId;
import static org.icgc.dcc.etl.db.importer.util.Genes.getGeneUniprotIds;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.gene.model.GeneGeneSet;
import org.icgc.dcc.etl.db.importer.go.model.GoAssociation;
import org.icgc.dcc.etl.db.importer.go.model.GoInferredTreeNode;
import org.icgc.dcc.etl.db.importer.go.model.GoModel;
import org.icgc.dcc.etl.db.importer.go.model.GoTerm;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

@Slf4j
public class GoGeneGeneSetsBuilder {

  /**
   * Data.
   */
  @NonNull
  private final Map<String, List<GoInferredTreeNode>> inferredTrees;

  /**
   * Indexed data.
   */
  @NonNull
  private final Map<String, GoTerm> goIdTermsIndex;
  @NonNull
  private final Multimap<String, GoAssociation> uniprotIdAssociationsIndex;
  @NonNull
  private final Multimap<String, GoAssociation> goIdAssociationsIndex;

  public GoGeneGeneSetsBuilder(@NonNull GoModel model) {
    this.inferredTrees = model.getInferredTrees();

    log.info("Indexing GO model...");
    this.goIdTermsIndex = GoTermIndexer.indexGoId(model.getTerms());
    this.uniprotIdAssociationsIndex = indexUniProtId(model.getAssociations());
    this.goIdAssociationsIndex = indexGoId(model.getAssociations());
  }

  public Set<GeneGeneSet> build(@NonNull ObjectNode gene) {
    val geneSets = Sets.<GeneGeneSet> newHashSet();
    val geneUniprotIds = getGeneUniprotIds(gene);
    log.debug("Uniprots IDs: {}", geneUniprotIds);

    for (val uniprotId : geneUniprotIds) {
      buildUniprotInferredGeneSets(geneSets, geneUniprotIds, uniprotId);
    }

    return geneSets;
  }

  private void buildUniprotInferredGeneSets(Set<GeneGeneSet> geneSets, Set<String> geneUniprotIds, String uniprotId) {
    val uniprotAssociations = uniprotIdAssociationsIndex.get(uniprotId);

    for (val uniprotAssociation : uniprotAssociations) {
      buildUniprotTermInferredGeneSets(geneSets, geneUniprotIds, uniprotAssociation.getKey().getGoId());
    }
  }

  private void buildUniprotTermInferredGeneSets(Set<GeneGeneSet> geneSets, Set<String> geneUniprotIds,
      String uniprotGoId) {
    val uniprotTermInferredTree = inferredTrees.get(uniprotGoId);
    if (uniprotTermInferredTree != null) {
      // Walk the tree
      for (val uniprotTermInferredTreeNode : uniprotTermInferredTree) {
        val geneSet = buildUniprotTermInferredTreeNodeGeneSet(geneUniprotIds, uniprotTermInferredTreeNode);
        geneSets.add(geneSet);
      }
    }

  }

  private GeneGeneSet buildUniprotTermInferredTreeNodeGeneSet(Set<String> geneUniprotIds,
      GoInferredTreeNode uniprotTermInferredTreeNode) {
    val uniprotTermInferredTreeTerm = goIdTermsIndex.get(uniprotTermInferredTreeNode.getId());
    val uniprotTermInferredTreeTermAssociations = goIdAssociationsIndex.get(uniprotTermInferredTreeTerm.getId());

    boolean direct = false;
    val uniqueQualifiers = Sets.<String> newHashSet();
    for (val uniprotTermInferredTreeTermAssociation : uniprotTermInferredTreeTermAssociations) {
      val inferredUniprotId = uniprotTermInferredTreeTermAssociation.getKey().getUniProtId();
      if (geneUniprotIds.contains(inferredUniprotId)) {
        direct = true;
        val qualifiers = uniprotTermInferredTreeTermAssociation.getQualifiers();

        if (qualifiers == null || qualifiers.isEmpty()) {
          uniqueQualifiers.add(null);
        } else {
          uniqueQualifiers.addAll(qualifiers);
        }
      }
    }

    return GeneGeneSet.builder()
        .id(uniprotTermInferredTreeTerm.getId())
        .name(uniprotTermInferredTreeTerm.getName())
        .type(GO_TERM)
        .annotation(direct ? DIRECT : INFERRED)
        .qualifiers(direct ? uniqueQualifiers : null)
        .build();
  }

}
