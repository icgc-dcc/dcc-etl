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
package org.icgc.dcc.etl.importer.go.reader;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static owltools.graph.RelationSets.REGULATES;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.go.model.GoInferredTreeNode;
import org.icgc.dcc.etl.importer.go.util.GoInferredTreeBuilder;
import org.obolibrary.oboformat.parser.OBOFormatParserException;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import owltools.graph.OWLGraphWrapper;
import owltools.graph.RelationSets;
import owltools.io.ParserWrapper;

import com.google.common.collect.ImmutableMap;

/**
 * Reads an "inferred tree" as defined by the Amigo project.
 * <p>
 * Requires 2 GB of heap.
 * 
 * @see https
 * ://code.google.com/p/owltools/source/browse/trunk/OWLTools-Runner/src/main/java/owltools/cli/SolrCommandRunner.java
 * @see https://github.com/azurebrd/wobr/blob/master/amigo.cgi#L449-L537
 * @see https://github.com/azurebrd/wobr/blob/master/amigo.cgi#L617-L654
 * @see https://github.com/azurebrd/wobr/blob/master/amigo.cgi#L656-L676
 * @see https://github.com/azurebrd/wobr/blob/master/amigo.cgi#L678-L703
 */
@Slf4j
@RequiredArgsConstructor
public class GoInferredTreeReader {

  /**
   * Constants.
   */
  public static final List<String> RELATION_IDS = RelationSets.getRelationSet(REGULATES);

  /**
   * Configuration.
   */
  @NonNull
  private final URL owlUrl;

  public Map<String, List<GoInferredTreeNode>> read() throws IOException, OBOFormatParserException,
      OWLOntologyCreationException {
    val watch = createStarted();

    log.info("Reading OWL graph...");
    val graph = readGraph();

    log.info("Creating GO term inferred trees...");
    val inferredTrees = createInferredTrees(graph);

    log.info("Finished processing {} GO term inferred trees in {}", formatCount(inferredTrees.size()), watch);
    return inferredTrees;
  }

  private Map<String, List<GoInferredTreeNode>> createInferredTrees(OWLGraphWrapper graph) {
    val watch = createStarted();
    val inferredTrees = ImmutableMap.<String, List<GoInferredTreeNode>> builder();
    int inferredTreeCount = 0;
    val inferredTreeBuilder = new GoInferredTreeBuilder(graph, RELATION_IDS);

    for (val goTerm : graph.getAllOWLClasses()) {
      // Build the inferred tree for the current term
      val inferredTree = inferredTreeBuilder.build(goTerm);

      val goId = graph.getIdentifier(goTerm);
      inferredTrees.put(goId, inferredTree);

      if (++inferredTreeCount % 1000 == 0) {
        log.info("Created {} GO term inferred trees in {}", formatCount(inferredTreeCount), watch);
      }
    }

    return inferredTrees.build();
  }

  public OWLGraphWrapper readGraph() throws OWLOntologyCreationException, IOException, OBOFormatParserException {
    log.info("Parsing GO OWL graph from {}...", owlUrl);
    return new ParserWrapper().parseToOWLGraph(owlUrl.toString());
  }

}
