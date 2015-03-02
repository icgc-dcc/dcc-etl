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
package org.icgc.dcc.etl.db.importer.cgc.writer;

import static org.icgc.dcc.etl.db.importer.geneset.model.GeneSetType.CURATED_SET;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.geneset.model.GeneSet;
import org.icgc.dcc.etl.db.importer.geneset.writer.AbstractGeneSetWriter;
import org.jongo.MongoCollection;

@Slf4j
public class CgcGeneSetWriter extends AbstractGeneSetWriter {

  /**
   * Constants.
   */
  public static final String CGS_GENE_SET_ID = "GS1";
  public static final String CGS_GENE_SET_SOURCE = "COSMIC - Sanger";
  public static final String CGS_GENE_SET_NAME = "Cancer Gene Census";
  public static final String CGS_GENE_SET_DESCRIPTION =
      "The cancer Gene Census is an ongoing effort to catalogue those genes for which mutations have been causally implicated in cancer. The original census and analysis was published in Nature Reviews Cancer and supplemental analysis information related to the paper is also available.";

  public CgcGeneSetWriter(@NonNull MongoCollection geneSetCollection) {
    super(geneSetCollection);
  }

  public void write() {
    log.info("Clearing CGC gene set documents...");
    clearGeneSets(CURATED_SET);

    log.info("Creating CGC gene set document...");
    val geneSet = createGeneSet();

    log.info("Writing CGC gene set documents...");
    saveGeneSet(geneSet);
  }

  private static GeneSet createGeneSet() {
    return GeneSet.builder()
        .id(CGS_GENE_SET_ID)
        .type(CURATED_SET)
        .source(CGS_GENE_SET_SOURCE)
        .name(CGS_GENE_SET_NAME)
        .description(CGS_GENE_SET_DESCRIPTION)
        .curatedSet(null) // Not used yet
        .build();
  }

}
