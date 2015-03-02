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
package org.icgc.dcc.etl.db.importer.go.util;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.filter;
import static lombok.AccessLevel.PRIVATE;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.db.importer.go.model.GoInferredTreeNode;
import org.icgc.dcc.etl.db.importer.go.model.GoTerm;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

@NoArgsConstructor(access = PRIVATE)
public final class GoTermFilter {

  public static Iterable<GoTerm> filterObsoleteTerms(@NonNull Iterable<GoTerm> terms) {
    return filterTerms(terms, true);
  }

  public static Iterable<GoTerm> filterCurrentTerms(@NonNull Iterable<GoTerm> terms) {
    return filterTerms(terms, false);
  }

  private static Iterable<GoTerm> filterTerms(Iterable<GoTerm> terms, final boolean obsolete) {
    return copyOf(filter(terms, new Predicate<GoTerm>() {

      @Override
      public boolean apply(GoTerm term) {
        return term.isObsolete() == obsolete;
      }

    }));
  }

  public static Iterable<GoTerm> filterGoTermByInferredTrees(Iterable<GoTerm> terms,
      Map<String, List<GoInferredTreeNode>> inferredTrees) {
    val inferredGoTerms = inferredTrees.keySet();
    val filteredGoTerms = ImmutableSet.<GoTerm> builder();
    for (val goTerm : terms) {
      if (inferredGoTerms.contains(goTerm.getId())) {
        filteredGoTerms.add(goTerm);
      }
    }

    return filteredGoTerms.build();
  }

  public static Iterable<GoTerm> filterGoTermByGoTermIds(Iterable<GoTerm> terms, Set<String> goTermIds) {
    val filteredGoTerms = ImmutableSet.<GoTerm> builder();
    for (val goTerm : terms) {
      if (goTermIds.contains(goTerm.getId())) {
        filteredGoTerms.add(goTerm);
      }
    }

    return filteredGoTerms.build();
  }

}
