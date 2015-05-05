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
package org.icgc.dcc.etl.db.importer.go;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.URLs.getUrl;
import static org.icgc.dcc.etl.db.importer.go.util.GoTermAncestorResolver.resolveTermAncestors;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.cli.CollectionName;
import org.icgc.dcc.etl.db.importer.core.Importer;
import org.icgc.dcc.etl.db.importer.go.core.GoAssociationProcessor;
import org.icgc.dcc.etl.db.importer.go.core.GoTermProcessor;
import org.icgc.dcc.etl.db.importer.go.model.GoAssociation;
import org.icgc.dcc.etl.db.importer.go.model.GoInferredTreeNode;
import org.icgc.dcc.etl.db.importer.go.model.GoModel;
import org.icgc.dcc.etl.db.importer.go.model.GoTerm;
import org.icgc.dcc.etl.db.importer.go.reader.GoAssociationReader;
import org.icgc.dcc.etl.db.importer.go.reader.GoInferredTreeReader;
import org.icgc.dcc.etl.db.importer.go.reader.GoTermReader;
import org.icgc.dcc.etl.db.importer.go.util.GoInferredTrees;
import org.icgc.dcc.etl.db.importer.go.writer.GoWriter;
import org.obolibrary.oboformat.parser.OBOFormatParserException;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import com.mongodb.MongoClientURI;

/**
 * Gene Ontology (GO) importer.
 * <p>
 * This class needs at least 1GB of heap to successfully parse and maintain both OBO and GAF documents in memory.
 * 
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/Gene+Set+-+GO+integration+-+Gene+Ontology+Database
 */
@Slf4j
@RequiredArgsConstructor
public class GoImporter implements Importer {

  /**
   * These files contain the core GO ontology in two formats, OBO and OWL-RDF/XML. This view includes relationships not
   * in the filtered version of GO including has_part and occurs_in. The set of relationships used is likely to extend
   * over time. Many of these relationships may not be safe for propagating annotations across, so this version should
   * not be used with legacy GO tools. This version currently excludes relationships to external ontologies.
   */
  public static final URL DEFAULT_OBO_URL =
      getUrl("http://purl.obolibrary.org/obo/go.obo");
  public static final URL DEFAULT_OWL_URL =
      getUrl("http://geneontology.org/ontology/go.owl");

  /**
   * This is the basic version of the GO filtered such that the graph is guaranteed to be acyclic, and annotations can
   * be propagated up the graph. The relations included are is_a, part_of, regulates, negatively_regulates and
   * positively_regulates. This version excludes relationships that cross the 3 main GO hierarchies. This is the version
   * that should be used with most GO-based annotation tools.
   */
  public static final URL DEFAULT_BASIC_OBO_URL =
      getUrl("http://purl.obolibrary.org/obo/go/go-basic.obo");

  /**
   * These files contains all GO annotations and information for a species subset of proteins in the UniProt
   * KnowledgeBase (UniProtKB) and for entities other than proteins, e.g. macromolecular complexes (IntAct Complex
   * Portal identifiers) or RNAs (RNAcentral identifiers). These files may provide annotations to more than one protein
   * per gene. The protein accessions included in these files are all Swiss-Prot entries for that species plus any
   * TrEMBL entries that have an Ensembl DR line. The TrEMBL entries are likely to overlap with the Swiss-Prot entries
   * or their isoforms. If a particular entity is not annotated with GO, then it will not appear in this file. The file
   * is provided as GAF2.0 format (http://www.geneontology.org/GO.format.gaf-2_0.shtml).
   */
  public static final URL DEFAULT_GAF_URL =
      getUrl("http://geneontology.org/gene-associations/gene_association.goa_human.gz");

  /**
   * Configuration.
   */
  @NonNull
  private final URL oboUrl;
  @NonNull
  private final URL gafUrl;
  @NonNull
  private final URL owlUrl;
  @NonNull
  private final MongoClientURI mongoUri;
  @NonNull
  private final URL[] ontologies;

  public GoImporter(@NonNull MongoClientURI mongoUri) {
    this.oboUrl = DEFAULT_OBO_URL;
    this.gafUrl = DEFAULT_GAF_URL;
    this.owlUrl = DEFAULT_OWL_URL;
    this.ontologies = new URL[] {
        DEFAULT_OWL_URL
    };
    this.mongoUri = mongoUri;
  }

  @Override
  public CollectionName getCollectionName() {
    return CollectionName.PROJECTS;
  }

  @Override
  @SneakyThrows
  public void execute() {
    val watch = createStarted();

    log.info("Reading inferred trees...");
    val inferredTrees = readInferredTrees(ontologies);
    log.info("Number of nodes in inferred trees: {}", GoInferredTrees.inferredTreeNodeCount(inferredTrees));

    log.info("Processing terms...");
    val terms = processTerms(oboUrl);

    log.info("Resolving term ancestors...");
    val termAncestors = resolveTermAncestors(terms);

    log.info("Processing associations...");
    val associations = processAssociations(gafUrl);

    log.info("Creating model...");
    val model = GoModel.builder()
        .terms(terms)
        .termAncestors(termAncestors)
        .associations(associations)
        .inferredTrees(inferredTrees)
        .build();

    log.info("Pruning model...");
    model.prune();

    log.info("Persisting model...");
    persistModel(model, mongoUri);

    log.info("Imported {} terms and {} associations in {}.",
        formatCount(terms), formatCount(associations), watch);
  }

  private static Map<String, List<GoInferredTreeNode>> readInferredTree(URL owlUrl) throws OBOFormatParserException,
      OWLOntologyCreationException, IOException {
    return new GoInferredTreeReader(owlUrl).read();
  }

  private static Map<String, List<GoInferredTreeNode>> readInferredTrees(URL[] ontologyURLS)
      throws OBOFormatParserException,
      OWLOntologyCreationException, IOException {
    val inferredTrees = new HashMap<String, List<GoInferredTreeNode>>();

    for (val ontologyURL : ontologyURLS) {
      val inferredTree = readInferredTree(ontologyURL);
      for (val entry : inferredTree.entrySet()) {
        val key = entry.getKey();
        val value = entry.getValue();
        if (inferredTrees.get(key) == null) {
          inferredTrees.put(key, value);
        } else {
          value.addAll(inferredTrees.get(key));
          inferredTrees.put(key, value);
        }
      }
      inferredTrees.putAll(inferredTree);
    }

    return inferredTrees;
  }

  private static Iterable<GoTerm> processTerms(URL oboUrl) throws IOException, OBOFormatParserException {
    return new GoTermProcessor(new GoTermReader(oboUrl)).process();
  }

  private static Iterable<GoAssociation> processAssociations(URL gafUrl) throws IOException, URISyntaxException {
    return new GoAssociationProcessor(new GoAssociationReader(gafUrl)).process();
  }

  private static void persistModel(GoModel model, MongoClientURI mongoUri) throws UnknownHostException, IOException {
    @Cleanup
    val writer = new GoWriter(mongoUri);
    writer.write(model);
  }

}
