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
package org.icgc.dcc.etl.importer.pathway.core;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static lombok.AccessLevel.NONE;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.pathway.model.Pathway;
import org.icgc.dcc.etl.importer.pathway.model.PathwaySegment;
import org.icgc.dcc.etl.importer.pathway.model.PathwaySummation;
import org.icgc.dcc.etl.importer.pathway.model.PathwayUniprot;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

@Data
@Builder
@Getter(NONE)
@Slf4j
public class PathwayModel {

  /**
   * State.
   */
  @NonNull
  Iterable<PathwaySummation> summations;
  @NonNull
  Iterable<PathwayUniprot> uniprots;
  @NonNull
  Multimap<String, List<PathwaySegment>> hierarchies;
  @NonNull
  Map<String, Pathway> pathways;

  /**
   * Indexes.
   */
  BiMap<String, String> reactomeIdByName;
  Map<String, Boolean> reactomeHasDiagramByName;
  Multimap<String, Pathway> pathwaysByUniprot;

  public void update() {

    //
    // Order matters for all operations below
    //

    log.info("Creating summation pathways...");
    createSummationPathways();
    log.info("Creating uniprot pathways...");
    createUniprotPathways();

    log.info("Indexing reactome ids...");
    indexReactomeNameReactomeId();
    log.info("Updating reactome ids...");
    updateReactomeIds();

    log.info("Indexing reactome names...");
    indexReactomeNameDiagrammed();
    log.info("Indexing pathway uniprots...");
    indexPathwayUniprots();
  }

  public Iterable<String> getReactomeIds() {
    return pathways.keySet();
  }

  public Pathway getPathway(String reactomeId) {
    return pathways.get(reactomeId);
  }

  public Iterable<Pathway> getPathways(String uniprot) {
    return pathwaysByUniprot.get(uniprot);
  }

  public Collection<List<PathwaySegment>> getHierarchy(String reactomeName) {
    return hierarchies.get(reactomeName);
  }

  public boolean hasDiagram(String reactomeName) {
    return firstNonNull(reactomeHasDiagramByName.get(reactomeName), false);
  }

  public String getReactomeName(String reactomeId) {
    return reactomeIdByName.inverse().get(reactomeId);
  }

  public String getReactomeId(String reactomeName) {
    return reactomeIdByName.get(reactomeName);
  }

  private void indexReactomeNameReactomeId() {
    reactomeIdByName = HashBiMap.create();
    for (val summation : summations) {
      reactomeIdByName.put(summation.getReactomeName(), summation.getReactomeId());
    }
  }

  private void indexReactomeNameDiagrammed() {
    reactomeHasDiagramByName = newHashMap();
    for (val reactomeName : hierarchies.keySet()) {
      for (val path : hierarchies.get(reactomeName)) {
        for (val segment : path) {
          reactomeHasDiagramByName.put(segment.getReactomeName(), segment.isDiagrammed());
        }
      }
    }
  }

  private void indexPathwayUniprots() {
    pathwaysByUniprot = HashMultimap.create();
    for (val reactomeId : pathways.keySet()) {
      val pathway = pathways.get(reactomeId);
      for (val uniprotId : pathway.getUniprots()) {
        pathwaysByUniprot.put(uniprotId, pathway);
      }
    }
  }

  private void createSummationPathways() {
    for (val summation : summations) {
      Pathway pathway = pathways.get(summation.getReactomeId());
      if (pathway == null) {
        pathway = new Pathway();
      }

      pathway.setReactomeId(summation.getReactomeId());
      pathway.setReactomeName(summation.getReactomeName());
      pathway.setSummation(summation.getSummation());

      pathways.put(pathway.getReactomeId(), pathway);
    }
  }

  private void createUniprotPathways() {
    for (val uniprot : uniprots) {
      Pathway pathway = pathways.get(uniprot.getReactomeId());
      if (pathway == null) {
        pathway = new Pathway();
      }

      pathway.setReactomeId(uniprot.getReactomeId());
      pathway.setEvidenceCode(uniprot.getEvidenceCode());
      pathway.getUniprots().add(uniprot.getUniprot());

      pathways.put(pathway.getReactomeId(), pathway);
    }
  }

  private void updateReactomeIds() {
    // Identify Reactome names with Reactome ids
    for (val reactomeName : hierarchies.keySet()) {
      val pathwayHierarchy = getHierarchy(reactomeName);
      for (val path : pathwayHierarchy) {
        for (val pathwaySegment : path) {
          val reactomeId = getReactomeId(pathwaySegment.getReactomeName());
          checkNotNull(reactomeId,
              "Cannot find reactome id for pathway segment with reactome name '%s' and segment '%s'",
              pathwaySegment.getReactomeName(), pathwaySegment);

          pathwaySegment.setReactomeId(reactomeId);
        }
      }
    }
  }

}
