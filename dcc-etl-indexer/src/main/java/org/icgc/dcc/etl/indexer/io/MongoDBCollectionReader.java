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
package org.icgc.dcc.etl.indexer.io;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_CHROMOSOME;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_CHROMOSOME_END;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_CHROMOSOME_START;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_MUTATION_ID;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DONOR_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_SET_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.MUTATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.RELEASE_COLLECTION;
import static org.icgc.dcc.common.core.util.MongoDbUtils.fields;
import static org.icgc.dcc.etl.indexer.model.CollectionFields.collectionFields;
import static org.icgc.dcc.etl.indexer.transform.GeneGeneSetPivoter.pivotGenesGeneSets;

import java.io.IOException;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.etl.indexer.core.CollectionReader;
import org.icgc.dcc.etl.indexer.model.CollectionFields;
import org.jongo.Find;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.QueryModifier;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.mongodb.DBCursor;

/**
 * Data access layer for document collection sources.
 */
@RequiredArgsConstructor
public class MongoDBCollectionReader implements CollectionReader {

  /**
   * Data access.
   */
  @NonNull
  private final Jongo client;

  @Override
  public Iterable<ObjectNode> readReleases(@NonNull CollectionFields fields) {
    return read(RELEASE_COLLECTION, fields);
  }

  @Override
  public Iterable<ObjectNode> readProjects(@NonNull CollectionFields fields) {
    return read(PROJECT_COLLECTION, fields);
  }

  @Override
  public Iterable<ObjectNode> readDonors(@NonNull CollectionFields fields) {
    return read(DONOR_COLLECTION, fields);
  }

  @Override
  public Iterable<ObjectNode> readGenes(@NonNull CollectionFields fields) {
    return read(getCollection(GENE_COLLECTION).find(), fields);
  }

  @Override
  public Iterable<ObjectNode> readGenesPivoted(@NonNull CollectionFields fields) {
    val genes = readGenes(fields);

    // TODO: This is somewhat of hack, but it was the most expeditious way to get the desired result
    return pivotGenesGeneSets(genes, readGeneSetOntologies());
  }

  @Override
  public Iterable<ObjectNode> readGeneSets(@NonNull CollectionFields fields) {
    return read(GENE_SET_COLLECTION, fields);
  }

  @Override
  public Iterable<ObjectNode> readObservations(@NonNull CollectionFields fields) {
    return read(OBSERVATION_COLLECTION, fields);
  }

  @Override
  public Iterable<ObjectNode> readObservationsByDonorId(@NonNull String donorId, CollectionFields fields) {
    return read(getCollection(OBSERVATION_COLLECTION).find("{ " + OBSERVATION_DONOR_ID + ": # }", donorId), fields);
  }

  @Override
  public Iterable<ObjectNode> readObservationsByGeneId(@NonNull String geneId, @NonNull CollectionFields fields) {
    val query = "{ " + OBSERVATION_CONSEQUENCES + ": { $elemMatch: { " + OBSERVATION_CONSEQUENCES_GENE_ID + ": # } } }";
    return read(getCollection(OBSERVATION_COLLECTION).find(query, geneId), fields);
  }

  @Override
  public Iterable<ObjectNode> readObservationsByMutationId(String mutationId, CollectionFields fields) {
    return read(getCollection(OBSERVATION_COLLECTION).find("{ " + OBSERVATION_MUTATION_ID + ": # }", mutationId),
        fields);
  }

  @Override
  public Iterable<ObjectNode> readMutations(@NonNull CollectionFields fields) {
    // Required ordering for VCF file creation (see MutationVCFDocumentWriter)
    val sortFields =
        "{ " + MUTATION_CHROMOSOME + ": 1, " + MUTATION_CHROMOSOME_START + ": 1, " + MUTATION_CHROMOSOME_END + ": 1 }";
    return read(getCollection(MUTATION_COLLECTION).find().sort(sortFields), fields);
  }

  @Override
  public Iterable<ObjectNode> read(@NonNull ReleaseCollection releaseCollection, @NonNull CollectionFields fields) {
    if (releaseCollection == MUTATION_COLLECTION) {
      // NOTE: Special case
      return readMutations(fields);
    } else if (releaseCollection == GENE_COLLECTION) {
      // NOTE: Special case
      return readGenesPivoted(fields);
    } else {
      return read(getCollection(releaseCollection).find(), fields);
    }
  }

  @Override
  public void close() throws IOException {
    client.getDatabase().getMongo().close();
  }

  protected Map<String, String> readGeneSetOntologies() {
    val geneSets = readGeneSets(collectionFields().includedFields("id", "go_term.ontology").build());

    val geneSetOntologies = ImmutableMap.<String, String> builder();
    for (val geneSet : geneSets) {
      val goTerm = geneSet.path("go_term");

      if (!goTerm.isMissingNode()) {
        val id = geneSet.get("id").textValue();
        val ontology = goTerm.get("ontology").textValue();

        geneSetOntologies.put(id, ontology);
      }
    }

    return geneSetOntologies.build();
  }

  protected static Iterable<ObjectNode> read(@NonNull Find find, @NonNull CollectionFields fields) {
    return find.projection(getFields(fields)).with(new QueryModifier() {

      @Override
      public void modify(DBCursor cursor) {
        // Prevent time outs due to idle cursors after an inactivity period (10 minutes)
        cursor.setOptions(QUERYOPTION_NOTIMEOUT);
        cursor.batchSize(Integer.MAX_VALUE);
      }

    }).as(ObjectNode.class);
  }

  protected MongoCollection getCollection(@NonNull ReleaseCollection releaseCollection) {
    return client.getCollection(releaseCollection.getId());
  }

  protected static String getFields(@NonNull CollectionFields fields) {
    return fields(copyOf(fields.getExcludedFields()), copyOf(fields.getIncludedFields()));
  }

}
