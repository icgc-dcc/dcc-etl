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
package org.icgc.dcc.etl.indexer.transform;

import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.indexer.core.Document;
import org.icgc.dcc.etl.indexer.core.DocumentContext;
import org.icgc.dcc.etl.indexer.core.DocumentTransform;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * {@link DocumentTransform} implementation that creates a gene document.
 */
public class GeneTextDocumentTransform implements DocumentTransform {

  @Override
  public Document transformDocument(@NonNull ObjectNode gene, @NonNull DocumentContext context) {
    // Identifiers
    val geneId = gene.get(GENE_ID).asText();
    val externalDbIds = gene.get("external_db_ids");

    val uniprotkbSwissprot = externalDbIds.get("uniprotkb_swissprot");
    val omimGene = externalDbIds.get("omim_gene");
    val entrezGene = externalDbIds.get("entrez_gene");
    val hgnc = externalDbIds.get("hgnc");
    val ensemblTranscriptId = externalDbIds.get("Ensembl_transcript_id");
    val ensemblTranslationId = externalDbIds.get("Ensembl_translation_id");

    gene.put("id", geneId);
    gene.put("uniprotkbSwissprot", uniprotkbSwissprot);
    gene.put("omimGene", omimGene);
    gene.put("entrezGene", entrezGene);
    gene.put("hgnc", hgnc);
    gene.put("ensemblTranscriptId", ensemblTranscriptId);
    gene.put("ensemblTranslationId", ensemblTranslationId);

    gene.put("type", "gene");

    gene.remove("external_db_ids");
    gene.remove(GENE_ID);
    return new Document(context.getType(), geneId, gene);
  }

}
