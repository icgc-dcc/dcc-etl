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

import static com.google.common.collect.Multimaps.index;
import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import org.icgc.dcc.etl.importer.go.model.GoAssociation;
import org.icgc.dcc.etl.importer.go.model.GoAssociationKey;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;

@NoArgsConstructor(access = PRIVATE)
public final class GoAssociationIndexer {

  public static Multimap<GoAssociationKey, GoAssociation> indexAssociationKey(
      @NonNull Iterable<GoAssociation> associations) {
    return index(associations, new Function<GoAssociation, GoAssociationKey>() {

      @Override
      public GoAssociationKey apply(GoAssociation association) {
        return association.getKey();
      }

    });
  }

  public static Multimap<String, GoAssociation> indexGoId(@NonNull Iterable<GoAssociation> associations) {
    return index(associations, new Function<GoAssociation, String>() {

      @Override
      public String apply(GoAssociation association) {
        return association.getKey().getGoId();
      }

    });
  }

  public static Multimap<String, GoAssociation> indexUniProtId(@NonNull Iterable<GoAssociation> associations) {
    return index(associations, new Function<GoAssociation, String>() {

      @Override
      public String apply(GoAssociation association) {
        return association.getKey().getUniProtId();
      }

    });
  }

}
