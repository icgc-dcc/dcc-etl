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

import static org.icgc.dcc.etl.importer.go.util.GoInferredTrees.sortLabelDescending;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import owltools.graph.shunt.OWLShuntGraph;
import owltools.graph.shunt.OWLShuntNode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@Slf4j
@RequiredArgsConstructor
public class GoInferredTreeBuilder {

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

  /**
   * Lookup helpers.
   */
  Map<String, Map<String, List<String>>> topologyGraphLookup;
  Map<String, Map<String, List<String>>> transitivityGraphLookup;
  Map<String, List<String>> subjectToObject;
  Map<String, List<String>> objectToSubject;

  public List<GoInferredTreeNode> build(@NonNull OWLClass goTerm) {
    PrintStream out = System.out;
    try {
      // Quite period because OWL doesn't know its not polite to pollute stdout
      System.setOut(EMPTY_PRINT_STREAM);

      OWLShuntGraph regulatesTransitivityGraph = graph.getLineageShuntGraph(goTerm, relationIds);
      OWLShuntGraph topologyGraph = graph.getSegmentShuntGraph(goTerm, relationIds);

      // Quiet period over, resume sanity
      System.setOut(out);

      // 1) Get edge and node relations
      String goTermId = graph.getIdentifier(goTerm);
      String goTermName = graph.getLabel(goTerm) == null ? "No Label" : graph.getLabel(goTerm);

      // 2) Build lookup tables
      buildLookupTable(topologyGraph.edges);
      topologyGraphLookup = buildPredicateLookupTable(topologyGraph.edges);
      transitivityGraphLookup = buildPredicateLookupTable(regulatesTransitivityGraph.edges);

      List<List<GoInferredTreeNode>> result = rich_bracket_layout(goTermId, topologyGraph, regulatesTransitivityGraph);
      log.debug("InferredTree for {}: {}", goTermId, result);

      val inferredNodes = Lists.<GoInferredTreeNode> newArrayList();
      for (val level : result) {
        for (val inferredNode : level) {
          inferredNodes.add(inferredNode);
        }
      }

      inferredNodes.add(createSelfTreeNode(goTermId, goTermName));

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

  private HashMap<String, Map<String, List<String>>> buildPredicateLookupTable(Set<OWLShuntEdge> edges) {
    HashMap<String, Map<String, List<String>>> subjectToObjectToPredicate =
        Maps.<String, Map<String, List<String>>> newHashMap();

    for (OWLShuntEdge edge : edges) {
      String subject = edge.sub;
      String object = edge.obj;
      String predicate = edge.pred.replace(" ", "_");

      if (subjectToObjectToPredicate.get(subject) == null) {
        Map<String, List<String>> temp = Maps.<String, List<String>> newHashMap();
        subjectToObjectToPredicate.put(subject, temp);
      }

      if (subjectToObjectToPredicate.get(subject).get(object) == null) {
        Map<String, List<String>> map = subjectToObjectToPredicate.get(subject);
        List<String> predicates = map.get(predicate);
        if (predicates == null) {
          predicates = new ArrayList<String>();
        }
        predicates.add(predicate);
        map.put(object, predicates);
        subjectToObjectToPredicate.put(subject, map);
      }
    }

    return subjectToObjectToPredicate;
  }

  private void buildLookupTable(Set<OWLShuntEdge> edges) {
    Map<String, List<String>> subjectToObject = Maps.<String, List<String>> newHashMap();
    Map<String, List<String>> objectToSubject = Maps.<String, List<String>> newHashMap();

    for (OWLShuntEdge edge : edges) {
      String subject = edge.sub;
      String object = edge.obj;

      if (subjectToObject.get(subject) == null) {
        List<String> objects = new ArrayList<String>();
        subjectToObject.put(subject, objects);
      }
      List<String> objects = subjectToObject.get(subject);
      objects.add(object);
      subjectToObject.put(subject, objects);

      if (objectToSubject.get(object) == null) {
        List<String> subjects = new ArrayList<String>();
        objectToSubject.put(object, subjects);
      }
      List<String> subjects = objectToSubject.get(object);
      subjects.add(subject);
      objectToSubject.put(object, subjects);

    }

    this.subjectToObject = subjectToObject;
    this.objectToSubject = objectToSubject;
  }

  private List<String> getParents(String goId) {
    return this.subjectToObject.get(goId);
  }

  // private List<String> getChildren(String goId) {
  // return this.objectToSubject.get(goId);
  // }

  private OWLShuntNode getNode(String nodeId, OWLShuntGraph graph) {
    Iterator<OWLShuntNode> it = graph.nodes.iterator();
    while (it.hasNext()) {
      OWLShuntNode a = it.next();
      if (a.id.equalsIgnoreCase(nodeId)) {
        return a;
      }
    }
    return null;
  }

  private List<String> getPredicates(String subjectId, String objectId,
      Map<String, Map<String, List<String>>> lookup) {
    Map<String, List<String>> table = lookup.get(subjectId);
    if (table == null) {
      return new ArrayList<String>();
    }
    List<String> result = lookup.get(subjectId).get(objectId);
    if (result == null) {
      return new ArrayList<String>();
    }
    return result;
  }

  private List<List<String>> bracket_layout(String term_acc) {
    Map<String, Integer> max_node_dist_from_root = max_info_climber(term_acc);
    Map<Integer, List<String>> lvl_lists = Maps.<Integer, List<String>> newHashMap();

    for (String node_id : max_node_dist_from_root.keySet()) {
      Integer level = max_node_dist_from_root.get(node_id);
      if (lvl_lists.get(level) == null) {
        lvl_lists.put(level, new ArrayList<String>());
      }
      List<String> list = lvl_lists.get(level);
      list.add(node_id);
      lvl_lists.put(level, list);
    }

    List<List<String>> bracket_list = new ArrayList<List<String>>();

    List<Integer> levels = Lists.<Integer> newArrayList(lvl_lists.keySet());
    Collections.sort(levels);

    for (Integer level : levels) {
      List<String> bracket = new ArrayList<String>();
      for (String item : lvl_lists.get(level)) {
        bracket.add(item);
      }
      bracket_list.add(bracket);
    }

    List<List<String>> reversed_bracket_list = new ArrayList<List<String>>();

    for (int i = bracket_list.size() - 1; i >= 0; i--) {
      reversed_bracket_list.add(bracket_list.get(i));
    }

    // in ICGC we don't want the children in inferred tree.
    // List<String> c_nodes = getChildren(term_acc);
    // if (c_nodes != null && !c_nodes.isEmpty()) {
    // ArrayList<String> kid_bracket = new ArrayList<String>();
    // for (String c_id : c_nodes) {
    // kid_bracket.add(c_id);
    // }
    // reversed_bracket_list.add(kid_bracket);
    // }

    return reversed_bracket_list;
  }

  private Map<String, Integer> max_info_climber(String term_acc) {
    Set<String> current = Sets.<String> newHashSet();
    current.add(term_acc);
    Map<String, Integer> in_max_hist = Maps.<String, Integer> newHashMap();
    Map<String, Integer> in_enc_hist = Maps.<String, Integer> newHashMap();
    return max_info_climber_helper(current, 0, in_max_hist, in_enc_hist);
  }

  private Map<String, Integer> max_info_climber_helper(Set<String> curr_list, int curr_term_distance,
      Map<String, Integer> max_hist, Map<String, Integer> encounter_hist) {

    if (curr_list != null && curr_list.size() > 0) {
      for (String item : curr_list) {
        if (encounter_hist.get(item) == null) {
          encounter_hist.put(item, new Integer(1));
          max_hist.put(item, curr_term_distance);
        } else {
          if (max_hist.get(item) < curr_term_distance) {
            max_hist.put(item, curr_term_distance);
          }
        }
      }

      Set<String> next_round = new HashSet<String>();

      for (String item : curr_list) {
        List<String> parents = getParents(item);
        if (parents != null) {
          for (String parent_id : parents) {
            next_round.add(parent_id);
          }
        }
      }

      curr_term_distance++;
      max_info_climber_helper(next_round, curr_term_distance, max_hist, encounter_hist);
    }

    return max_hist;
  }

  private List<List<GoInferredTreeNode>> rich_bracket_layout(String term_acc, OWLShuntGraph topology_graph,
      OWLShuntGraph transitivity_graph) {

    List<List<String>> layout = bracket_layout(term_acc);
    List<List<GoInferredTreeNode>> bracket_list = new ArrayList<List<GoInferredTreeNode>>();

    for (int level = 0; level < layout.size(); level++) {
      List<String> layout_level = layout.get(level);
      List<GoInferredTreeNode> bracket = new ArrayList<GoInferredTreeNode>();
      for (String layout_item : layout_level) {
        String curr_acc = layout_item;
        String pred_id = "is_a";
        OWLShuntNode curr_node = getNode(curr_acc, topology_graph);
        String label = curr_node.lbl == null ? layout_item : curr_node.lbl;

        if (!curr_acc.equalsIgnoreCase(term_acc)) {
          List<String> trels = getPredicates(term_acc, curr_acc, this.transitivityGraphLookup);
          if (trels != null && !trels.isEmpty()) {
            pred_id = GoInferredTrees.getDominantRelation(trels);
          } else {
            List<String> drels = getPredicates(curr_acc, term_acc, this.topologyGraphLookup);
            pred_id = GoInferredTrees.getDominantRelation(drels);
          }
          GoInferredTreeNode node = GoInferredTreeNode.builder()
              .name(label)
              .level(level)
              .relation(pred_id)
              .id(curr_acc)
              .build();
          bracket.add(node);
        }
      }
      sortLabelDescending(bracket);
      bracket_list.add(bracket);
    }

    return bracket_list;
  }

}
