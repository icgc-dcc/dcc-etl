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

import static com.google.common.base.Objects.firstNonNull;
import static java.lang.Boolean.parseBoolean;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.commons.lang3.StringEscapeUtils.unescapeHtml4;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.db.importer.pathway.model.PathwaySegment;
import org.w3c.dom.Node;

@NoArgsConstructor(access = PRIVATE)
public final class PathwaySegmentConverter {

  /**
   * Reactome XML name constants.
   */
  private static final String REACTOME_DISPLAY_NAME_ATTRIBUTE_NAME = "displayName";
  private static final String REACTOME_HAS_DIAGRAM_ATTRIBUTE_NAME = "hasDiagram";

  public static PathwaySegment convertPathwayNode(@NonNull Node pathwayNode) {
    // Extract
    val name = unescapeHtml4(getAttributeValue(pathwayNode, REACTOME_DISPLAY_NAME_ATTRIBUTE_NAME));
    val hasDiagram =
        parseBoolean(firstNonNull(getAttributeValue(pathwayNode, REACTOME_HAS_DIAGRAM_ATTRIBUTE_NAME), "false"));

    return PathwaySegment.builder()
        .reactomeId(null)
        .reactomeName(name)
        .diagrammed(hasDiagram)
        .build();
  }

  private static String getAttributeValue(Node pathwayNode, String attributeName) {
    val attribute = pathwayNode.getAttributes().getNamedItem(attributeName);

    return attribute == null ? null : attribute.getNodeValue();
  }

}
