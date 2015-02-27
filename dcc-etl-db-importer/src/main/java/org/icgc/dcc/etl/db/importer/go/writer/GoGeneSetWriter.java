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
package org.icgc.dcc.etl.db.importer.go.writer;

import static org.icgc.dcc.etl.db.importer.geneset.model.GeneSetType.GO_TERM;

import java.util.List;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.geneset.model.GeneSet;
import org.icgc.dcc.etl.db.importer.geneset.model.GeneSetGoTerm;
import org.icgc.dcc.etl.db.importer.geneset.writer.AbstractGeneSetWriter;
import org.icgc.dcc.etl.db.importer.go.model.GoInferredTreeNode;
import org.icgc.dcc.etl.db.importer.go.model.GoModel;
import org.icgc.dcc.etl.db.importer.go.model.GoTerm;
import org.jongo.MongoCollection;

@Slf4j
public class GoGeneSetWriter extends AbstractGeneSetWriter {

  /**
   * Constants.
   */
  private static final String GENE_ONTOLOGY_SOURCE = "Gene Ontology";

  public GoGeneSetWriter(@NonNull MongoCollection geneSetCollection) {
    super(geneSetCollection);
  }

  public void write(@NonNull GoModel model) {
    log.info("Clearing term gene set documents...");
    clearGeneSets(GO_TERM);

    log.info("Writing term gene set documents...");
    for (val term : model.getTerms()) {
      val inferredTree = model.getInferredTree(term.getId());
      val geneSet = createGeneSet(term, inferredTree);

      saveGeneSet(geneSet);
    }
  }

  private GeneSet createGeneSet(GoTerm term, List<GoInferredTreeNode> inferredTree) {
    return GeneSet.builder()
        .id(term.getId())
        .type(GO_TERM)
        .source(GENE_ONTOLOGY_SOURCE)
        .name(term.getName())
        .description(term.getDef())
        .goTerm(GeneSetGoTerm.builder()
            .ontology(term.getNamespace())
            .altIds(term.getAltIds())
            .synonyms(term.getSynonym())
            .inferredTree(inferredTree)
            .build())
        .build();
  }

}
