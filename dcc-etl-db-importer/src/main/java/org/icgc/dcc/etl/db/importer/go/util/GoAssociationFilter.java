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

import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.db.importer.go.model.GoAssociation;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

@NoArgsConstructor(access = PRIVATE)
public final class GoAssociationFilter {

  public static Iterable<GoAssociation> filterNegativeAssociations(@NonNull Iterable<GoAssociation> associations) {
    return filterAssociations(associations, true);
  }

  public static Iterable<GoAssociation> filterPositiveAssociations(@NonNull Iterable<GoAssociation> associations) {
    return filterAssociations(associations, false);
  }

  private static Iterable<GoAssociation> filterAssociations(Iterable<GoAssociation> associations, final boolean negated) {

    return copyOf(filter(associations, new Predicate<GoAssociation>() {

      @Override
      public boolean apply(GoAssociation association) {
        return association.isNegated() == negated;
      }

    }));
  }

  public static Iterable<GoAssociation> filterAssociationsByGeneUniprotIds(
      @NonNull Iterable<GoAssociation> associations,
      @NonNull final Set<String> uniprotIds) {
    val filteredAssociations = ImmutableList.<GoAssociation> builder();

    for (val association : associations) {
      if (uniprotIds.contains(association.getKey().getUniProtId())) {
        filteredAssociations.add(association);
      }
    }

    return filteredAssociations.build();
  }

  public static Iterable<GoAssociation> filterAssociationsByGeneUniprotIds(
      @NonNull Iterable<GoAssociation> associations,
      @NonNull final Iterable<String> uniprotIds) {
    return copyOf(filter(associations, new Predicate<GoAssociation>() {

      @Override
      public boolean apply(GoAssociation association) {
        return Iterables.contains(uniprotIds, association.getKey().getUniProtId());
      }

    }));
  }

}
