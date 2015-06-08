/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.db.importer.diagram.reader;

import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.Jackson.DEFAULT;
import static org.icgc.dcc.etl.db.importer.diagram.reader.DiagramReader.REACTOME_BASE_URL;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import lombok.val;

import org.codehaus.jettison.json.JSONException;
import org.elasticsearch.common.collect.Lists;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;

public class DiagramProteinMapReader {

  /**
   * Constants.
   */
  private final static String PROTEIN_MAP_URL = REACTOME_BASE_URL + "getPhysicalToReferenceEntityMaps/%s";
  private final static String GENE_TYPE = "ReferenceGeneProduct";

  public Map<String, List<String>> readProteinMap(String pathwayId) throws IOException, JSONException {
    val result = DEFAULT.readTree(new URL(format(PROTEIN_MAP_URL, pathwayId)));

    val proteinMap = Maps.<String, List<String>> newHashMap();
    result.forEach(node -> {
      String dbId = node.get("peDbId").asText();
      List<String> referenceIds = getReferenceIds(node.get("refEntities"));
      if (!referenceIds.isEmpty()) {
        proteinMap.put(dbId, referenceIds);
      }
    });

    return proteinMap;
  }

  private List<String> getReferenceIds(JsonNode entities) {
    val referenceIds = Lists.<String> newArrayList();
    entities.forEach(node -> {
      if (node.get("schemaClass").asText().equalsIgnoreCase(GENE_TYPE)) {
        String uniprotId = node.get("displayName").asText();
        if (uniprotId.indexOf(" ") > 0) {
          referenceIds.add(uniprotId.substring(0, uniprotId.indexOf(" ")));
        } else {
          referenceIds.add(uniprotId);
        }
      }
    });

    return referenceIds;
  }

}