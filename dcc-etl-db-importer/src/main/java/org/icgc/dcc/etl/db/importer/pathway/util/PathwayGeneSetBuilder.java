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
package org.icgc.dcc.etl.db.importer.pathway.util;

import static org.icgc.dcc.etl.db.importer.geneset.model.GeneSetType.PATHWAY;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.etl.db.importer.geneset.model.GeneSet;
import org.icgc.dcc.etl.db.importer.geneset.model.GeneSetPathway;
import org.icgc.dcc.etl.db.importer.pathway.core.PathwayModel;

@RequiredArgsConstructor
public class PathwayGeneSetBuilder {

  /**
   * Constants.
   */
  private static final String PATHWAY_SOURCE = "Reactome";
  private static final String HOMO_SAPIEN = "Homo sapiens";

  @NonNull
  private final PathwayModel model;

  public GeneSet build(@NonNull String reactomeId) {
    val pathway = model.getPathway(reactomeId);
    val diagrammed = model.hasDiagram(pathway.getReactomeName());
    val hierarchy = model.getHierarchy(pathway.getReactomeName());

    return GeneSet.builder()
        .id(pathway.getReactomeId())
        .type(PATHWAY)
        .source(PATHWAY_SOURCE)
        .name(pathway.getReactomeName())
        .description(pathway.getSummation())
        .pathway(GeneSetPathway.builder()
            .evidenceCode(pathway.getEvidenceCode())
            .species(HOMO_SAPIEN)
            .diagrammed(diagrammed)
            .hierarchy(hierarchy)
            .build())
        .build();
  }
}
