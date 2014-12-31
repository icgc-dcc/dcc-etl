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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.transform;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.etl.importer.go.util.GoAssociationIndexer.indexAssociationKey;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.importer.go.model.GoAssociation;
import org.icgc.dcc.etl.importer.go.model.GoAssociationKey;

import com.google.common.base.Function;
import com.google.common.collect.Sets;

@NoArgsConstructor(access = PRIVATE)
public final class GoAssociationAggregator {

  public static Iterable<GoAssociation> aggregateAssociations(@NonNull Iterable<GoAssociation> associations) {
    val assocationKeyIndex = indexAssociationKey(associations);

    return copyOf(transform(assocationKeyIndex.keySet(), new Function<GoAssociationKey, GoAssociation>() {

      @Override
      public GoAssociation apply(GoAssociationKey associationKey) {
        val associations = assocationKeyIndex.get(associationKey);

        return aggregateKeyAssociations(associationKey, associations);
      }

    }));
  }

  private static GoAssociation aggregateKeyAssociations(GoAssociationKey associationKey,
      Iterable<GoAssociation> associations) {
    val uniqueQualifiers = Sets.<String> newTreeSet();
    val uniqueGeneSymbols = Sets.<String> newHashSet();
    val uniqueNegated = Sets.<Boolean> newHashSet();
    for (val association : associations) {
      uniqueQualifiers.addAll(association.getQualifiers());
      uniqueGeneSymbols.add(association.getGeneSymbol());
      uniqueNegated.add(association.isNegated());
    }

    checkState(uniqueGeneSymbols.size() == 1, "Found multiple gene symbols for associations %s", associations);

    return GoAssociation.builder()
        .key(associationKey)
        .qualifiers(uniqueQualifiers)
        .geneSymbol(getFirst(uniqueGeneSymbols, null))
        .negated(getFirst(uniqueNegated, null))
        .build();
  }

}
