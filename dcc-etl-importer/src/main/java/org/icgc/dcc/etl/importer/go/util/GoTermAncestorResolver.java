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
import static org.icgc.dcc.etl.importer.go.util.GoTermIndexer.indexGoId;

import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.go.model.GoTerm;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class GoTermAncestorResolver {

  public static Multimap<String, String> resolveTermAncestors(@NonNull Iterable<GoTerm> terms) {
    val termIndex = indexGoId(terms);

    val ancestors = LinkedHashMultimap.<String, String> create();
    for (val term : terms) {
      log.debug("Resolving term ancestors: {}", term.getId());
      resolveTermAncestors(term, ancestors, termIndex);
      for (val ancestorId : ancestors.get(term.getId())) {
        log.debug("   {} ->  {}", term.getId(), ancestorId);
      }
      log.debug("");
    }

    return ancestors;
  }

  private static void resolveTermAncestors(GoTerm term, Multimap<String, String> ancestors,
      Map<String, GoTerm> termIndex) {
    resolveTermAncestors(term, term, ancestors, termIndex);
  }

  private static void resolveTermAncestors(GoTerm term, GoTerm ancestor, Multimap<String, String> ancestors,
      Map<String, GoTerm> termIndex) {
    val parentIds = getParentIds(ancestor);
    for (val parentId : parentIds) {
      val parent = termIndex.get(parentId);
      ancestors.put(term.getId(), parentId);

      // Recurse up parent chain
      resolveTermAncestors(term, parent, ancestors, termIndex);
    }
  }

  private static ImmutableSet<String> getParentIds(GoTerm ancestor) {
    return ImmutableSet.<String> builder()
        .addAll(ancestor.getIsA())
        .addAll(ancestor.getRelationship())
        .addAll(ancestor.getUnionOf())
        .addAll(ancestor.getIntersectionOf())
        .build();
  }

}
