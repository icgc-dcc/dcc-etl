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
package org.icgc.dcc.etl.db.importer.diagram.model;

import static org.icgc.dcc.common.core.model.FieldNames.DIAGRAM_HIGHLIGHTS;
import static org.icgc.dcc.common.core.model.FieldNames.DIAGRAM_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DIAGRAM_PROTEIN_MAP;
import static org.icgc.dcc.common.core.model.FieldNames.DIAGRAM_PROTEIN_MAP_DB_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DIAGRAM_PROTEIN_MAP_UNIPROT_IDS;
import static org.icgc.dcc.common.core.model.FieldNames.DIAGRAM_XML;
import static org.icgc.dcc.common.core.model.FieldNames.MONGO_INTERNAL_ID;
import lombok.NonNull;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DiagramNodeConverter {

  public JsonNode convertDiagram(@NonNull Diagram diagram, @NonNull String id) {
    val mapper = new ObjectMapper();

    val node = mapper.createObjectNode();
    node.put(MONGO_INTERNAL_ID, id);
    node.put(DIAGRAM_ID, id);
    node.put(DIAGRAM_XML, diagram.diagram);
    node.putPOJO(DIAGRAM_HIGHLIGHTS, diagram.highlights);

    val proteinNode = mapper.createArrayNode();
    for (val entry : diagram.proteinMap.entrySet()) {
      val mapping = mapper.createObjectNode();
      mapping.put(DIAGRAM_PROTEIN_MAP_DB_ID, entry.getKey());
      mapping.putPOJO(DIAGRAM_PROTEIN_MAP_UNIPROT_IDS, entry.getValue());

      proteinNode.add(mapping);
    }

    if (!diagram.proteinMap.isEmpty()) {
      node.put(DIAGRAM_PROTEIN_MAP, proteinNode);
    }

    return node;
  }
}
