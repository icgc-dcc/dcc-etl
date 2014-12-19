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

import static lombok.AccessLevel.PRIVATE;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.importer.go.model.GoInferredTreeNode;

@NoArgsConstructor(access = PRIVATE)
public final class GoInferredTrees {

  /**
   * Constants.
   */
  private static final Comparator<GoInferredTreeNode> LABEL_DECENDING_COMPARATOR =
      new Comparator<GoInferredTreeNode>() {

        @Override
        public int compare(GoInferredTreeNode a, GoInferredTreeNode b) {
          return b.getName().compareTo(a.getName());
        }
      };

  public static void sortLabelDescending(@NonNull List<GoInferredTreeNode> inferredTree) {
    Collections.sort(inferredTree, LABEL_DECENDING_COMPARATOR);
  }

  private static final Comparator<GoInferredTreeNode> LEVEL_DECENDING_COMPARATOR =
      new Comparator<GoInferredTreeNode>() {

        @Override
        public int compare(GoInferredTreeNode a, GoInferredTreeNode b) {
          return b.getLevel() - a.getLevel();
        }

      };

  public static void sortLevelDescending(@NonNull List<GoInferredTreeNode> inferredTree) {
    Collections.sort(inferredTree, LEVEL_DECENDING_COMPARATOR);
  }

  public static String getInferredRelationship(@NonNull String child, @NonNull String parent) {
    String relation = "unknown";

    if (child.equals("is_a")) {
      if (parent.equals("is_a")) return "is_a";
      if (parent.equals("part_of")) return "part_of";
      if (parent.equals("regulates")) return "regulates";
      if (parent.equals("positively_regulates")) return "positively_regulates";
      if (parent.equals("negatively_regulates")) return "negatively_regulates";
      if (parent.equals("has_part")) return "has_part";
    } else if (child.equals("part_of")) {
      if (parent.equals("is_a")) return "part_of";
      if (parent.equals("part_of")) return "part_of";
    } else if (child.equals("regulates")) {
      if (parent.equals("is_a")) return "regulates";
      if (parent.equals("part_of")) return "regulates";
    } else if (child.equals("positively_regulates")) {
      if (parent.equals("is_a")) return "positively_regulates";
      if (parent.equals("part_of")) return "regulates";
    } else if (child.equals("negatively_regulates")) {
      if (parent.equals("is_a")) return "negatively_regulates";
      if (parent.equals("part_of")) return "regulates";
    } else if (child.equals("has_part")) {
      if (parent.equals("is_a")) return "has_part";
      if (parent.equals("has_part")) return "has_part";
    }
    return relation;
  }

  public static String getDominantRelation(@NonNull Collection<String> relations) {
    // if (relations.contains("unknown")) return "unknown";
    // if (relations.contains("occurs_in")) return "occurs_in";
    if (relations.contains("positively_regulates")) return "positively_regulates";
    if (relations.contains("negatively_regulates")) return "negatively_regulates";
    if (relations.contains("regulates")) return "regulates";
    if (relations.contains("part_of")) return "part_of";
    if (relations.contains("has_part")) return "has_part";
    if (relations.contains("is_a")) return "is_a";

    return "unknown";
  }

  public static int inferredTreeNodeCount(Map<String, List<GoInferredTreeNode>> inferredTrees) {
    int count = 0;
    for (val goId : inferredTrees.keySet()) {
      val inferredTree = inferredTrees.get(goId);
      count += inferredTree.size();
    }

    return count;
  }

}
