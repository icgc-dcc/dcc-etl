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

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONORS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_PROJECT;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SUMMARY_AFFECTED_DONOR_COUNT;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getDonorProjectId;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getGeneDonorId;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getGeneDonors;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getGeneId;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getObservationDonorId;
import static org.icgc.dcc.etl.indexer.model.CollectionFieldAccessors.getObservationType;
import static org.icgc.dcc.etl.indexer.util.JsonNodes.isEmpty;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.indexer.core.Document;
import org.icgc.dcc.etl.indexer.core.DocumentContext;
import org.icgc.dcc.etl.indexer.core.DocumentTransform;
import org.icgc.dcc.etl.indexer.util.Fakes;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;

/**
 * {@link DocumentTransform} implementation that creates a nested gene-centric document.
 */
public class GeneCentricDocumentTransform extends AbstractCentricDocumentTransform {

  @Override
  public Document transformDocument(@NonNull ObjectNode gene, @NonNull DocumentContext context) {
    // Index observations by donor id
    val geneId = getGeneId(gene);
    val geneObservations = context.getObservationsByGeneId(geneId);
    val geneDonorsObservations = indexGeneDonorsObservations(geneObservations);

    /**
     * Donors: {@code gene.donors}(s).
     */

    // Nest
    val geneDonors = getGeneDonors(gene);
    for (val geneDonorId : geneDonorsObservations.keySet()) {
      val donor = context.getDonor(geneDonorId).deepCopy();

      // Extract donor non-feature type summary values for merging
      val donorSummary = donor.has(DONOR_SUMMARY) ? (ObjectNode) donor.remove(DONOR_SUMMARY) : donor.objectNode();

      // Find gene donor and remove summary for merging
      val geneDonor = findGeneDonor(gene, geneDonors, geneDonorId);
      val geneDonorSummary = geneDonor.has(GENE_DONOR_SUMMARY) ?
          (ObjectNode) geneDonor.remove(GENE_DONOR_SUMMARY) : geneDonor.objectNode();

      // Merge (add donor's summary to donor summary of the gene's donor)
      geneDonorSummary.putAll(donorSummary);
      geneDonor.putAll(donor);
      geneDonor.put(GENE_DONOR_SUMMARY, geneDonorSummary);

      // Add donor-project info
      val donorProjectId = getDonorProjectId(donor);
      val donorProject = context.getProject(donorProjectId).deepCopy();
      geneDonor.put(GENE_DONOR_PROJECT, donorProject);

      // Merge
      val geneDonorObservations = geneDonorsObservations.get(geneDonorId);
      val geneDonorTree = createGeneDonorTree(gene, donor, geneDonorObservations);
      geneDonor.putAll(geneDonorTree);
    }

    if (isEmpty(geneDonors)) {
      // Ensure arrays are present with at least 1 element
      geneDonors.add(Fakes.createPlaceholder());
    }

    /**
     * Summary: {@code gene._summary}.
     */

    val summary = gene.with(GENE_SUMMARY);

    val affectedDonorCount = getAffectedDonorCount(gene);
    summary.put(GENE_SUMMARY_AFFECTED_DONOR_COUNT, affectedDonorCount);

    return new Document(context.getType(), geneId, gene);
  }

  private static ObjectNode createGeneDonorTree(ObjectNode gene, ObjectNode geneDonor,
      Iterable<ObjectNode> geneDonorObservations) {

    // Process each observation associated with the current donor
    for (val geneDonorObservation : geneDonorObservations) {
      // Add to feature type array (e.g. "ssm")
      val observationType = getObservationType(geneDonorObservation);
      val array = geneDonor.withArray(observationType);

      // Remove unrelated gene consequences and aggregate
      transformGeneObservationConsequences(gene, geneDonorObservation);

      array.add(geneDonorObservation);
    }

    return geneDonor;
  }

  private static ObjectNode findGeneDonor(Object gene, ArrayNode geneDonors, Object geneDonorId) {
    for (val value : geneDonors) {
      val geneDonor = (ObjectNode) value;

      // Is it an id match?
      val idMatch = getGeneDonorId(geneDonor).equals(geneDonorId);
      if (idMatch) {
        return geneDonor;
      }
    }

    throw new RuntimeException("No gene donor summary found for donor id '" + geneDonorId + "' and gene " + gene);
  }

  private static int getAffectedDonorCount(ObjectNode gene) {
    val geneDonors = gene.withArray(GENE_DONORS);
    int rawAffectedDonorCount = geneDonors.size();

    int placeholderDonorCount = 0;
    for (val geneDonor : geneDonors) {
      if (Fakes.isPlaceholder(geneDonor)) {
        placeholderDonorCount++;
      }
    }

    // We don't want to include placeholders as they are purely synthetic.
    // See https://jira.oicr.on.ca/browse/DCC-2506
    val realAffectedDonorCount = rawAffectedDonorCount - placeholderDonorCount;

    return realAffectedDonorCount;
  }

  private static Multimap<String, ObjectNode> indexGeneDonorsObservations(@NonNull Iterable<ObjectNode> observations) {
    // Set and multi-semantics are required
    val index = ImmutableSetMultimap.<String, ObjectNode> builder();
    for (val observation : observations) {
      val donorId = getObservationDonorId(observation);

      index.put(donorId, observation);
    }

    return index.build();
  }

}
