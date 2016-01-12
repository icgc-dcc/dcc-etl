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
package org.icgc.dcc.etl.indexer.model;

import static org.icgc.dcc.common.core.model.ReleaseCollection.DONOR_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_SET_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.MUTATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.RELEASE_COLLECTION;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.Entity;
import org.icgc.dcc.common.core.model.IndexType;
import org.icgc.dcc.common.core.model.IndexType.Classifier;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.etl.indexer.core.DocumentTransform;
import org.icgc.dcc.etl.indexer.transform.DonorCentricDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.DonorDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.DonorTextDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.DrugTextDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.GeneCentricDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.GeneSetDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.GeneSetTextDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.GeneTextDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.MutationCentricDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.MutationTextDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.ObservationCentricDocumentTransform;
import org.icgc.dcc.etl.indexer.transform.ProjectTextDocumentTransform;

import com.google.common.collect.ImmutableList;

/**
 * Metadata for document types.
 * <p/>
 * Eventually this entire class can be externalized and data driven.
 */
@Getter
public enum DocumentType {

  /**
   * Diagram type(s).
   */
  DIAGRAM_TYPE(attributes()
      .indexType(IndexType.DIAGRAM_TYPE)
      .collection(ReleaseCollection.DIAGRAM_COLLECTION)
      .batchSize(100)
      .statusInterval(100)
  ),

  /**
   * Drug type(s).
   */
  DRUG_TEXT_TYPE(attributes()
      .indexType(IndexType.DRUG_TEXT_TYPE)
      .collection(ReleaseCollection.DRUG_COLLECTION)
      .transform(new DrugTextDocumentTransform())
      .batchSize(100)
      .statusInterval(100)
      .fields(
          fields()
              .drugFields(
                  drugFields()
                      .excludedFields(
                          "cancer_trial_count",
                          "genes",
                          "large_image_url",
                          "small_image_url",
                          "_id")
              )
      )),

  DRUG_CENTRIC_TYPE(attributes()
      .indexType(IndexType.DRUG_CENTRIC_TYPE)
      .collection(ReleaseCollection.DRUG_COLLECTION)
      .batchSize(100)
      .statusInterval(100)),

  /**
   * Release type(s).
   */
  RELEASE_TYPE(attributes()
      .indexType(IndexType.RELEASE_TYPE)
      .collection(RELEASE_COLLECTION)
      .batchSize(1000)
      .statusInterval(1)
  ),

  /**
   * Gene Set type(s).
   */
  GENE_SET_TYPE(attributes()
      .indexType(IndexType.GENE_SET_TYPE)
      .collection(GENE_SET_COLLECTION)
      .transform(new GeneSetDocumentTransform())
      .statusInterval(1000)
      .batchSize(1000)
      .fields(
          fields()
              .geneFields(
                  geneFields()
                      .includedFields(
                          "_gene_id",

                          // Gene sets
                          "sets.id",
                          "sets.type",

                          "project._project_id",
                          "donor")
              )
              .projectFields(
                  projectFields()
                      .includedFields(
                          "_project_id",
                          "project_name",
                          "primary_site",
                          "tumour_type",
                          "tumour_subtype",
                          "_summary._ssm_tested_donor_count",
                          "_summary._total_donor_count")
              )
      )),
  GENE_SET_TEXT_TYPE(attributes()
      .indexType(IndexType.GENE_SET_TEXT_TYPE)
      .collection(GENE_SET_COLLECTION)
      .transform(new GeneSetTextDocumentTransform())
      .statusInterval(1000)
      .batchSize(1000)
      .fields(
          fields()
              .geneSetFields(
                  geneSetFields()
                      .includedFields(
                          "id",
                          "name",
                          "type",
                          "source",
                          "go_term.alt_ids")
              )
      )),

  /**
   * Project type(s).
   */
  PROJECT_TYPE(attributes()
      .indexType(IndexType.PROJECT_TYPE)
      .collection(PROJECT_COLLECTION)
      .batchSize(100)
      .statusInterval(10)
  ),
  PROJECT_TEXT_TYPE(attributes()
      .indexType(IndexType.PROJECT_TEXT_TYPE)
      .collection(PROJECT_COLLECTION)
      .transform(new ProjectTextDocumentTransform())
      .batchSize(100)
      .statusInterval(10)
      .fields(
          fields()
              .projectFields(
                  projectFields()
                      .includedFields(
                          "_project_id",
                          "project_name",
                          "tumour_type",
                          "tumour_subtype",
                          "primary_site",
                          "_summary._state")
              )
      )
  ),

  /**
   * Donor type(s).
   */
  DONOR_TYPE(attributes()
      .indexType(IndexType.DONOR_TYPE)
      .collection(DONOR_COLLECTION)
      .transform(new DonorDocumentTransform())
      .batchSize(10000)
      .statusInterval(1000)
      .fields(
          fields()
              .donorFields(
                  donorFields()
                      .excludedFields(
                          "_id",
                          "gene")
              )
      )
  ),
  DONOR_TEXT_TYPE(attributes()
      .indexType(IndexType.DONOR_TEXT_TYPE)
      .collection(DONOR_COLLECTION)
      .transform(new DonorTextDocumentTransform())
      .batchSize(10000)
      .statusInterval(1000)
      .fields(
          fields()
              .donorFields(
                  donorFields()
                      .includedFields(
                          "_donor_id",
                          "_project_id",
                          "donor_id",
                          "specimen._specimen_id",
                          "specimen.specimen_id",
                          "specimen.sample._sample_id",
                          "specimen.sample.analyzed_sample_id",
                          "_summary._state")
              )
      )
  ),
  DONOR_CENTRIC_TYPE(attributes()
      .indexType(IndexType.DONOR_CENTRIC_TYPE)
      .collection(DONOR_COLLECTION)
      .transform(new DonorCentricDocumentTransform())
      .statusInterval(1000)
      .batchSize(1)
      .fields(
          fields()
              .projectFields(
                  projectFields()
                      .includedFields(
                          // Primary key
                          "_project_id",

                          // Data
                          "primary_site")
              )
              .donorFields(
                  donorFields()
                      .includedFields(
                          // Primary key
                          "_donor_id",

                          // Foreign keys
                          "_project_id",

                          // Summary
                          "gene",
                          "_summary",

                          // Data
                          "disease_status_last_followup",
                          "donor_age_at_diagnosis",
                          "donor_relapse_type",
                          "donor_sex",
                          "donor_survival_time",
                          "donor_tumour_stage_at_diagnosis",
                          "donor_vital_status")
              )
              .geneFields(
                  geneFields()
                      .includedFields(
                          // Primary key
                          "_gene_id",

                          // Data
                          "symbol",
                          "biotype",
                          "chromosome",
                          "start",
                          "end",

                          // Gene sets
                          "sets.id",
                          "sets.type")
              )
              .observationFields(
                  observationFields()
                      .includedFields(
                          // Foreign keys
                          "_mutation_id",
                          "consequence._gene_id", // Don't index

                          // Data
                          "consequence.consequence_type",
                          "consequence.functional_impact_prediction_summary",
                          "_type",
                          "mutation_type",
                          "chromosome",
                          "chromosome_end",
                          "chromosome_start",
                          "observation.platform",
                          "observation.sequencing_strategy",
                          "observation.verification_status"
                      )
              )
      )
  ),

  /**
   * Gene type(s).
   */
  GENE_TYPE(attributes()
      .indexType(IndexType.GENE_TYPE)
      .collection(GENE_COLLECTION)
      .batchSize(1000)
      .statusInterval(1000)
      .fields(
          fields()
              .geneFields(geneFields()
                  .excludedFields("donor")
              )
              .observationFields(
                  observationFields()
                      .excludedFields(
                          "_id",
                          "_summary",
                          "project",
                          "donor")
              )
      )
  ),
  GENE_TEXT_TYPE(attributes()
      .indexType(IndexType.GENE_TEXT_TYPE)
      .collection(GENE_COLLECTION)
      .transform(new GeneTextDocumentTransform())
      .batchSize(1000)
      .statusInterval(1000)
      .fields(
          fields()
              .geneFields(
                  geneFields()
                      .includedFields(
                          "_gene_id",
                          "symbol",
                          "name",
                          "synonyms",
                          "external_db_ids")
              )
      )
  ),
  GENE_CENTRIC_TYPE(attributes()
      .indexType(IndexType.GENE_CENTRIC_TYPE)
      .collection(GENE_COLLECTION)
      .transform(new GeneCentricDocumentTransform())
      .batchSize(1000)
      .statusInterval(1000)
      .fields(
          fields()
              .projectFields(
                  projectFields()
                      .includedFields(
                          "_project_id",
                          "primary_site"
                      )
              )
              .donorFields(
                  donorFields()
                      .includedFields(
                          "_donor_id",
                          "_project_id",
                          "_summary._age_at_diagnosis_group",
                          "_summary._available_data_type",
                          "_summary._state",
                          "_summary._studies",
                          "_summary.experimental_analysis_performed",
                          "disease_status_last_followup",
                          "donor_relapse_type",
                          "donor_sex",
                          "donor_tumour_stage_at_diagnosis",
                          "donor_vital_status"
                      )
              )
              .observationFields(
                  observationFields()
                      .includedFields(
                          "_mutation_id",
                          "_donor_id", // Don't index
                          "_type", // Don't index
                          "chromosome",
                          "chromosome_end",
                          "chromosome_start",
                          "consequence._gene_id",
                          "consequence.consequence_type",
                          "consequence.transcript_affected",
                          "consequence.functional_impact_prediction_summary",
                          "mutation_type",
                          "observation.platform",
                          "observation.sequencing_strategy",
                          "observation.verification_status"
                      )
              )
              .geneFields(
                  geneFields()
                      .excludedFields(
                          "_id",
                          "canonical_transcript_id",
                          "description",
                          "external_db_ids",
                          "project",
                          "strand",
                          "synonyms",
                          "transcripts"
                      )
              )
      )
  ),

  /**
   * Observation type(s).
   */
  OBSERVATION_CENTRIC_TYPE(attributes()
      .indexType(IndexType.OBSERVATION_CENTRIC_TYPE)
      .collection(OBSERVATION_COLLECTION)
      .transform(new ObservationCentricDocumentTransform())
      .batchSize(200)
      .statusInterval(100000)
      .fields(
          fields()
              .projectFields(
                  projectFields()
                      .includedFields(
                          "_project_id",
                          "primary_site")
              )
              .donorFields(
                  donorFields()
                      .includedFields(
                          "_donor_id",
                          "_project_id", // FK. Don't index

                          // Data
                          "donor_sex",
                          "donor_tumour_stage_at_diagnosis",
                          "donor_vital_status",
                          "disease_status_last_followup",
                          "donor_relapse_type",
                          "_summary._age_at_diagnosis_group",
                          "_summary._available_data_type",
                          "_summary._studies",
                          "_summary.experimental_analysis_performed",
                          "_summary._state")
              )
              .geneFields(
                  geneFields()
                      .includedFields(
                          "_gene_id",
                          "biotype",
                          "chromosome",
                          "start",
                          "end",
                          "sets")

              )
              .observationFields(
                  observationFields()
                      .excludedFields(
                          "_id",
                          "assembly_version",
                          "mutated_to_allele",
                          "mutated_from_allele",
                          "reference_genome_allele",
                          "chromosome_strand",
                          "functional_impact_prediction_summary",
                          "_project_id",

                          // Consequence
                          "consequence.gene_build_version",
                          "consequence.transcript_affected",
                          "consequence.gene_affected",
                          "consequence._transcript_id",
                          "consequence.functional_impact_prediction",

                          // Observation
                          "observation.analysis_id",
                          "observation.quality_score",
                          "observation.verification_platform",
                          "observation.marking",
                          "observation.observation_id",
                          "observation.seq_coverage")
              )
      )
  ),

  /**
   * Mutation type(s).
   */
  MUTATION_TEXT_TYPE(attributes()
      .indexType(IndexType.MUTATION_TEXT_TYPE)
      .collection(MUTATION_COLLECTION)
      .transform(new MutationTextDocumentTransform())
      .batchSize(1000)
      .statusInterval(100000)
      .fields(
          fields()
              .mutationFields(
                  mutationFields()
                      .includedFields(
                          "_mutation_id",
                          "mutation",
                          "chromosome",
                          "chromosome_start")
              )

              .observationFields(
                  observationFields()
                      .includedFields(
                          "_mutation_id",
                          "consequence._gene_id",
                          "consequence.aa_mutation",
                          "mutation")
              )
              .geneFields(
                  geneFields()
                      .includedFields(
                          "_gene_id",
                          "symbol")
              )
      )
  ),
  MUTATION_CENTRIC_TYPE(attributes()
      .indexType(IndexType.MUTATION_CENTRIC_TYPE)
      .collection(MUTATION_COLLECTION)
      .transform(new MutationCentricDocumentTransform())
      .batchSize(1000)
      .statusInterval(100000)
      .fields(
          fields()
              .projectFields(
                  projectFields()
                      .includedFields(
                          "_project_id",
                          "primary_site"
                      )
              )
              .donorFields(
                  donorFields()
                      .includedFields(
                          "_donor_id",
                          "disease_status_last_followup",
                          "donor_relapse_type",
                          "donor_sex",
                          "donor_tumour_stage_at_diagnosis",
                          "donor_vital_status",
                          "_summary._age_at_diagnosis_group",
                          "_summary._available_data_type",
                          "_summary._state",
                          "_summary._studies",
                          "_summary.experimental_analysis_performed",
                          "_project_id" // Used as a foreign key
                      )
              )
              .geneFields(
                  geneFields()
                      .includedFields(
                          "_gene_id",
                          "biotype",
                          "chromosome",
                          "end",
                          "sets",
                          "start",
                          "symbol",
                          "transcripts.id",
                          "transcripts.name"
                      )

              )
              .observationFields(
                  observationFields()
                      .includedFields(
                          "_donor_id",
                          "consequence._gene_id", // Don't index
                          "consequence._transcript_id", // Don't index
                          "consequence.consequence_type",
                          "consequence.aa_mutation",
                          "consequence.gene_affected",
                          "consequence.functional_impact_prediction_summary",
                          "observation.platform",
                          "observation.sequencing_strategy",
                          "observation.verification_status"
                      )
              )
              .mutationFields(
                  mutationFields()
                      .excludedFields(
                          "_id"
                      )
              )
      )
  );

  /**
   * The corresponding entity of the index type.
   */
  // TODO: Seems not to be used.
  private final Entity entity;

  /**
   * The name of the index type.
   */
  private final String name;

  /**
   * The classifier of the index type.
   */
  private final Classifier classifier;

  /**
   * The document transform.
   */
  private final DocumentTransform transform;

  /**
   * The document status interval.
   */
  private final int statusInterval;

  /**
   * The document batch size.
   */
  private final int batchSize;

  /**
   * The source collection.
   */
  private final ReleaseCollection collection;

  /**
   * The source collection fields used to create the index.
   */
  private final DocumentFields fields;

  private DocumentType(@NonNull DocumentTypeAttributes attributes) {
    val indexType = attributes.indexType;
    this.entity = indexType.getEntity();
    this.name = indexType.getName();
    this.classifier = indexType.getClassifier();
    this.transform = attributes.transform;
    this.batchSize = attributes.batchSize;
    this.statusInterval = attributes.statusInterval;
    this.collection = attributes.collection;
    this.fields = attributes.fields;
  }

  public static Iterable<DocumentType> convert(Iterable<IndexType> indexTypes) {
    val types = ImmutableList.<DocumentType> builder();
    for (val indexType : indexTypes) {
      types.add(DocumentType.byName(indexType.getName()));
    }

    return types.build();
  }

  public static DocumentType byName(@NonNull String name) {
    for (val value : values()) {
      if (name.equals(value.name)) {
        return value;
      }
    }

    throw new IllegalArgumentException("No '" + DocumentType.class.getName() + "' value with name '" + name + "' found");
  }

  @Override
  public String toString() {
    return name;
  }

  private static DocumentTypeAttributes attributes() {
    return new DocumentTypeAttributes();
  }

  private static DocumentFields.Builder fields() {
    return DocumentFields.documentFields();
  }

  private static CollectionFields.Builder geneFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder projectFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder donorFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder observationFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder mutationFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder geneSetFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder drugFields() {
    return CollectionFields.collectionFields();
  }

}
