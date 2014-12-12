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

import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.NonNull;

import org.icgc.dcc.etl.importer.go.model.GoAssociation;
import org.icgc.dcc.etl.importer.go.model.GoAssociationKey;

import owltools.gaf.GeneAnnotation;

import com.google.common.collect.Sets;

@NoArgsConstructor(access = PRIVATE)
public final class GoAssociationConverter {

  public static GoAssociation convertGeneAnnotation(@NonNull GeneAnnotation annotation) {
    return GoAssociation.builder()
        .key(convertAssociationKey(annotation))
        .geneSymbol(annotation.getBioentityObject().getSymbol())
        .qualifiers(convertQualifiers(annotation))
        .negated(annotation.isNegated())
        .build();
  }

  private static GoAssociationKey convertAssociationKey(GeneAnnotation annotation) {
    return GoAssociationKey.builder()
        .uniProtId(annotation.getBioentityObject().getDBID())
        .goId(annotation.getCls())
        .build();
  }

  private static Set<String> convertQualifiers(GeneAnnotation annotation) {
    // Unique and sorted lexicographically
    return Sets.newTreeSet(annotation.getCompositeQualifiers());
  }

}
