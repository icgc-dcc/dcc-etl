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
package org.icgc.dcc.etl.db.importer.go.model;

import static com.google.common.base.Stopwatch.createStarted;

import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.NonNull;
import lombok.val;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.go.util.GoAssociationIndexer;
import org.icgc.dcc.etl.db.importer.go.util.GoInferredTreeFilter;
import org.icgc.dcc.etl.db.importer.go.util.GoTermFilter;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

@Data
@Builder
@Slf4j
public class GoModel {

  @NonNull
  Iterable<GoTerm> terms;
  @NonNull
  Multimap<String, String> termAncestors;
  @NonNull
  Map<String, List<GoInferredTreeNode>> inferredTrees;

  @NonNull
  Iterable<GoAssociation> associations;

  public List<GoInferredTreeNode> getInferredTree(String goId) {
    return inferredTrees.get(goId);
  }

  public void prune() {
    log.info("Pruning started....");
    val watch = createStarted();

    val goIdsInAssociations = GoAssociationIndexer.indexGoId(associations).keySet();
    log.info("Number of goIds in associations: {}", goIdsInAssociations.size());

    log.info("Number of goIds in inferredTrees before pruning: {}", Iterables.size(inferredTrees.keySet()));
    val directAndInferredGoIds =
        GoInferredTreeFilter.filterByDirectAndInferredGoIds(inferredTrees, goIdsInAssociations);
    log.info("Number of goIds in inferredTrees after pruning: {}", Iterables.size(directAndInferredGoIds));

    log.info("Number of terms before pruning: {}", Iterables.size(terms));
    this.terms = GoTermFilter.filterGoTermByGoTermIds(terms, directAndInferredGoIds);
    log.info("Number of terms after pruning: {}", Iterables.size(terms));

    log.info("Pruning finished in {}.", watch);
  }

}
