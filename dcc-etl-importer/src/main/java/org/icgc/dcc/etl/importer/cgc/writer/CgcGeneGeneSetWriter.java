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
package org.icgc.dcc.etl.importer.cgc.writer;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SYMBOL;
import static org.icgc.dcc.etl.importer.cgc.model.CgcGene.CGC_GENE_SYMBOL_FIELD_NAME;
import static org.icgc.dcc.etl.importer.cgc.writer.CgcGeneSetWriter.CGS_GENE_SET_ID;
import static org.icgc.dcc.etl.importer.cgc.writer.CgcGeneSetWriter.CGS_GENE_SET_NAME;
import static org.icgc.dcc.etl.importer.geneset.model.GeneSetAnnotation.DIRECT;
import static org.icgc.dcc.etl.importer.geneset.model.GeneSetType.CURATED_SET;

import java.util.Map;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.gene.model.GeneGeneSet;
import org.icgc.dcc.etl.importer.gene.writer.AbstractGeneGeneSetWriter;
import org.jongo.MongoCollection;

@Slf4j
public class CgcGeneGeneSetWriter extends AbstractGeneGeneSetWriter {

  public CgcGeneGeneSetWriter(@NonNull MongoCollection geneCollection) {
    super(geneCollection, CURATED_SET);
  }

  public void write(@NonNull Iterable<Map<String, String>> cgc) {
    log.info("Clearing gene CGC gene sets...");
    clearGeneGeneSets();

    log.info("Updating gene CGC gene sets...");
    val count = updateGeneGeneSets(cgc, geneCollection);
    log.info("Updated {} gene CGC gene sets...", count);
  }

  private int updateGeneGeneSets(Iterable<Map<String, String>> cgc, MongoCollection geneCollection) {
    val geneGeneSet = createGeneGeneSet();
    int count = 0;
    for (val cgcGene : cgc) {
      val geneSymbol = cgcGene.get(CGC_GENE_SYMBOL_FIELD_NAME);
      checkNotNull(geneSymbol, "gene symbol is missing.");
      val result = geneCollection.update("{ " + GENE_SYMBOL + ": # }", geneSymbol)
          .multi()
          .with("{ $addToSet: { " + type.getFieldName() + ": # } }", geneGeneSet);
      count += result.getN();
    }

    return count;
  }

  private static GeneGeneSet createGeneGeneSet() {
    return GeneGeneSet.builder()
        .id(CGS_GENE_SET_ID)
        .name(CGS_GENE_SET_NAME)
        .type(CURATED_SET)
        .annotation(DIRECT)
        .build();
  }

}
