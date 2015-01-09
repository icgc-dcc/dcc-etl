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

      List<List<GoInferredTreeNode>> result = richBracketLayout(goTermId, topologyGraph, regulatesTransitivityGraph);
      log.debug("InferredTree for {}: {}", goTermId, result);

      val inferredNodes = Lists.<GoInferredTreeNode> newArrayList();
      for (val level : result) {
        for (val inferredNode : level) {
          inferredNodes.add(inferredNode);
        }
      }

      val self = GoInferredTreeNode.builder()
          .name(goTermName)
          .level(result.size())
          .relation("self")
          .id(goTermId)
          .build();

      inferredNodes.add(self);

      return inferredNodes;
    } finally {
      System.setOut(out);
    }
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

  private List<List<String>> bracketLayout(String term_acc) {
    Map<String, Integer> maxNodeDistanceFromRoot = maxInfoClimber(term_acc);
    Map<Integer, List<String>> levelLists = Maps.<Integer, List<String>> newHashMap();

    for (String goId : maxNodeDistanceFromRoot.keySet()) {
      Integer level = maxNodeDistanceFromRoot.get(goId);
      if (levelLists.get(level) == null) {
        levelLists.put(level, new ArrayList<String>());
      }
      List<String> list = levelLists.get(level);
      list.add(goId);
      levelLists.put(level, list);
    }

    List<List<String>> bracketList = new ArrayList<List<String>>();
    List<Integer> levels = Lists.<Integer> newArrayList(levelLists.keySet());
    Collections.sort(levels);

    for (Integer level : levels) {
      List<String> bracket = new ArrayList<String>();
      for (String item : levelLists.get(level)) {
        bracket.add(item);
      }
      bracketList.add(bracket);
    }

    Collections.reverse(bracketList);

    return bracketList;
  }

  private Map<String, Integer> maxInfoClimber(String goId) {
    Set<String> currentSet = Sets.<String> newHashSet();
    currentSet.add(goId);
    return maxInfoClimberHelper(currentSet, 0, Maps.<String, Integer> newHashMap(), Maps.<String, Integer> newHashMap());
  }

  private Map<String, Integer> maxInfoClimberHelper(Set<String> currentSet, int currentTermDistance,
      Map<String, Integer> completeHistory, Map<String, Integer> encounteredHistory) {

    if (!currentSet.isEmpty()) {
      for (String item : currentSet) {
        if (encounteredHistory.get(item) == null) {
          encounteredHistory.put(item, new Integer(1));
          completeHistory.put(item, currentTermDistance);
        } else {
          if (completeHistory.get(item) < currentTermDistance) {
            completeHistory.put(item, currentTermDistance);
          }
        }
      }
      Set<String> nextSet = new HashSet<String>();
      for (String item : currentSet) {
        List<String> parents = getParents(item);
        if (parents != null) {
          for (String parentId : parents) {
            nextSet.add(parentId);
          }
        }
      }
      currentTermDistance++;
      maxInfoClimberHelper(nextSet, currentTermDistance, completeHistory, encounteredHistory);
    }

    return completeHistory;
  }

  private List<List<GoInferredTreeNode>> richBracketLayout(String goId, OWLShuntGraph topologyGraph,
      OWLShuntGraph transitivityGraph) {

    List<List<String>> layout = bracketLayout(goId);
    List<List<GoInferredTreeNode>> bracketList = new ArrayList<List<GoInferredTreeNode>>();

    for (int level = 0; level < layout.size(); level++) {
      List<String> layoutLevel = layout.get(level);
      List<GoInferredTreeNode> bracket = new ArrayList<GoInferredTreeNode>();
      for (String layoutItem : layoutLevel) {
        String currentGoId = layoutItem;
        String predicate = "is_a";
        OWLShuntNode currentNode = getNode(currentGoId, topologyGraph);
        String label = currentNode.lbl == null ? layoutItem : currentNode.lbl;
        if (!currentGoId.equalsIgnoreCase(goId)) {
          List<String> trels = getPredicates(goId, currentGoId, this.transitivityGraphLookup);
          if (trels != null && !trels.isEmpty()) {
            predicate = GoInferredTrees.getDominantRelation(trels);
          } else {
            List<String> drels = getPredicates(currentGoId, goId, this.topologyGraphLookup);
            predicate = GoInferredTrees.getDominantRelation(drels);
          }
          GoInferredTreeNode node = GoInferredTreeNode.builder()
              .name(label)
              .level(level)
              .relation(predicate)
              .id(currentGoId)
              .build();
          bracket.add(node);
        }
      }
      sortLabelDescending(bracket);
      bracketList.add(bracket);
    }

    return bracketList;
  }

}
