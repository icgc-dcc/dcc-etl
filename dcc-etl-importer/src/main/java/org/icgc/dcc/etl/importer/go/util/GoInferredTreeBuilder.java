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
package org.icgc.dcc.etl.importer.go.util;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.icgc.dcc.etl.importer.go.util.GoInferredTrees.sortLevelDescending;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.go.model.GoInferredTreeNode;
import org.semanticweb.owlapi.model.OWLClass;

import owltools.graph.OWLGraphWrapper;
import owltools.graph.shunt.OWLShuntEdge;
import owltools.graph.shunt.OWLShuntNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@Slf4j
@RequiredArgsConstructor
public class GoInferredTreeBuilder {

  /**
   * Constants.
   */
  private static final String CHILD_TO_PARENT = "childToParent";
  private static final String PARENT_TO_CHILD = "parentToChild";
  private static final Set<String> NON_TRANSITIVE_PREDICATE = ImmutableSet.of(
      "daughter_of", "daughter_of_in_hermaphrodite", "daughter_of_in_male", "develops_from", "exclusive_union_of");
  private static final PrintStream EMPTY_PRINT_STREAM = new PrintStream(new OutputStream() {

    @Override
    public void write(int arg0) throws IOException {
      // No-op
    }

  });

  /**
   * Data.
   */
  @NonNull
  private final OWLGraphWrapper graph;

  /**
   * Configuration.
   */
  @NonNull
  private final List<String> relationIds;

  public List<GoInferredTreeNode> build(@NonNull OWLClass goTerm) {
    val out = System.out;
    try {
      // Quite period because OWL doesn't know its not polite to pollute stdout
      System.setOut(EMPTY_PRINT_STREAM);

      // This is the basis for the inferred tree
      val regulatesTransitivityGraph = graph.getLineageShuntGraph(goTerm, relationIds);

      // This may not be needed
      val topologyGraph = graph.getSegmentShuntGraph(goTerm, relationIds);

      // Quiet period over, resume sanity
      System.setOut(out);

      log.debug("Topology graph: {}", topologyGraph);

      // 1) Get edge and node relations
      val goTermId = graph.getIdentifier(goTerm);
      val goTermName = graph.getLabel(goTerm);
      val edges = topologyGraph.edges;
      val nodes = regulatesTransitivityGraph.nodes;

      // 2) Build relation tables
      val lookup = buildLookuptable(edges, goTermId);
      val ancestors = getAncestors(goTermId, nodes);

      // 3) For each of the go-term's ancestor, find the maximum traversal distance, find the most dominant relation
      // between go-term and the ancestor node. Note the most dominant relation is not necessary on the longest path.
      val inferredNodes = Lists.<GoInferredTreeNode> newArrayList();
      for (val ancestor : ancestors) {
        log.debug("Ancestor: {}", ancestor.id);
        val inferredNode = getLongestPathAndTransivity(lookup, goTermId, ancestor.id, ancestor.lbl);
        if (inferredNode.getLevel() < 0) {
          // TODO: Determine why we are seeing these!
          continue;
        }

        inferredNodes.add(inferredNode);
      }

      // 4) Add one-self to the inferred tree
      val createSelfTreeNode = createSelfTreeNode(goTermId, goTermName);
      inferredNodes.add(createSelfTreeNode);

      // 5) Sort by steps in descending order in terms of distance, to store the tree in level-wise structure
      sortLevelDescending(inferredNodes);

      log.debug("{}", buildInferredTree(inferredNodes));

      return inferredNodes;
    } finally {
      System.setOut(out);
    }
  }

  private GoInferredTreeNode createSelfTreeNode(String goTermId, String goTermName) {
    return GoInferredTreeNode.builder()
        .name(goTermName)
        .level(0)
        .relation("self")
        .id(goTermId)
        .build();
  }

  private Set<OWLShuntNode> getAncestors(String goId, Set<OWLShuntNode> nodes) {
    val ancestors = Sets.<OWLShuntNode> newHashSet();
    for (val node : nodes) {
      val self = node.getId().equals(goId);
      if (!self) {
        ancestors.add(node);
      }
    }

    return ancestors;
  }

  /**
   * Given an edge map, build a child-parent and parent-child lookup table
   */
  private Map<String, Map<String, Map<String, String>>> buildLookuptable(Set<OWLShuntEdge> edges, String id) {
    val result = Maps.<String, Map<String, Map<String, String>>> newHashMap();
    val children = Maps.<String, String> newHashMap();
    val parents = Maps.<String, String> newHashMap();

    // Get children and parent relations
    //

    for (val edge : edges) {
      val subject = edge.sub;
      val object = edge.obj;
      // val predicate = edge.pred;
      val predicate = edge.pred.replace(" ", "_");

      if (!isNullOrEmpty(object) && object.equals(id)) {
        children.put(subject, predicate);
      }
      if (!isNullOrEmpty(subject) && subject.equals(id)) {
        parents.put(object, predicate);
      }
    }

    // Create a path map
    // https://github.com/azurebrd/wobr/blob/master/amigo.cgi#L353
    result.put(CHILD_TO_PARENT, Maps.<String, Map<String, String>> newHashMap());
    result.put(PARENT_TO_CHILD, Maps.<String, Map<String, String>> newHashMap());

    for (val edge : edges) {
      val subject = edge.sub;
      val object = edge.obj;
      // val predicate = edge.pred;
      val predicate = edge.pred.replaceAll(" ", "_");

      log.debug("Path {} {} {}", subject, object, predicate);

      if (isNullOrEmpty(subject) || isNullOrEmpty(object) || isNullOrEmpty(predicate)) {
        continue;
      }
      if (children.containsKey(subject)) {
        continue;
      }

      if (!result.get(CHILD_TO_PARENT).containsKey(subject)) {
        result.get(CHILD_TO_PARENT).put(subject, Maps.<String, String> newHashMap());
      }
      result.get(CHILD_TO_PARENT).get(subject).put(object, predicate);

    }
    return result;
  }

  private String inferPathRelation(Map<String, Map<String, Map<String, String>>> lookup, List<String> path) {
    log.debug("Inferring relation for {}", path);

    String child = path.get(0);
    String parent = path.get(1);
    String relationOne = lookup.get(CHILD_TO_PARENT).get(child).get(parent);
    String relationTwo = "";

    for (int idx = 2; idx < path.size(); idx++) {
      child = parent;
      parent = path.get(idx);
      relationTwo = lookup.get(CHILD_TO_PARENT).get(child).get(parent);

      log.debug("> {} {}", relationOne, relationTwo);
      relationOne = GoInferredTrees.getInferredRelationship(relationOne, relationTwo);

    }
    return relationOne;
  }

  private GoInferredTreeNode getLongestPathAndTransivity(Map<String, Map<String, Map<String, String>>> lookup,
      String goTermId, String ancestorId, String ancestorName) {
    val current = Lists.<String> newArrayList(goTermId);
    val finalPaths = Lists.<List<String>> newArrayList();
    val aggregatedRelations = Sets.<String> newHashSet();

    recurseLongestPath(
        lookup, // Child-parent table
        goTermId, goTermId, ancestorId, // Current node, start node, end node
        current, // Current path
        finalPaths); // All path resulting from start => end

    log.debug("Looking for longest path in {}", finalPaths);

    int maxNodes = 0;
    for (val path : finalPaths) {
      if (path.size() > maxNodes) {
        maxNodes = path.size();
      }

      aggregatedRelations.add(inferPathRelation(lookup, path));
    }

    val maxSteps = maxNodes - 1;
    val dominantInferredTransitivity = GoInferredTrees.getDominantRelation(aggregatedRelations);

    return GoInferredTreeNode.builder()
        .id(ancestorId)
        .name(ancestorName)
        .relation(dominantInferredTransitivity)
        .level(maxSteps)
        .build();
  }

  private void recurseLongestPath(Map<String, Map<String, Map<String, String>>> lookup, String current, String start,
      String end, List<String> currentPath, List<List<String>> finalPath) {

    if (!lookup.get(CHILD_TO_PARENT).containsKey(current)) {
      return;
    }

    val parents = lookup.get(CHILD_TO_PARENT).get(current);

    if (parents.size() <= 0) {
      return;
    }

    for (val parentKey : parents.keySet()) {
      if (NON_TRANSITIVE_PREDICATE.contains(lookup.get(CHILD_TO_PARENT).get(current).get(parentKey))) {
        continue;
      }

      val currentPathCopy = Lists.newArrayList(currentPath);
      currentPathCopy.add(parentKey);

      if (parentKey.equals(end)) {
        finalPath.add(currentPathCopy);
      } else {
        recurseLongestPath(lookup, parentKey, start, end, currentPathCopy, finalPath);
      }
    }
  }

  private List<List<Map<String, String>>> buildInferredTree(List<GoInferredTreeNode> list) {
    val result = ImmutableList.<List<Map<String, String>>> builder();

    List<Map<String, String>> levelList = Collections.emptyList();
    int current = -1;
    for (val node : list) {
      val map = Maps.<String, String> newHashMap();
      map.put("id", node.getId());
      map.put("name", node.getName());
      map.put("relation", node.getRelation());

      if (current != node.getLevel()) {
        current = node.getLevel();
        levelList = Lists.<Map<String, String>> newArrayList();
        result.add(levelList);
      }

      levelList.add(map);
    }

    return result.build();
  }

}
