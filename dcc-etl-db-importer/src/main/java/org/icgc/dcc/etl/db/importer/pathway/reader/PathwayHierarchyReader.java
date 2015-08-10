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
package org.icgc.dcc.etl.db.importer.pathway.reader;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.db.importer.pathway.util.PathwaySegmentConverter.convertPathwayNode;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Stack;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.pathway.model.PathwaySegment;
import org.icgc.dcc.etl.db.importer.util.Documents;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

@Slf4j
public class PathwayHierarchyReader {

  /**
   * Reactome XML name constants.
   */
  private static final String REACTOME_PATHWAY_ELEMENT_NAME = "Pathway";

  /**
   * Parses the supplied Reactome pathway hierarchy {@code hierarchyFile} to produce a mapping from pathway names to a
   * set of {@link PathwaySegment} lists.
   */
  public Multimap<String, List<PathwaySegment>> read(URI hierarchyFile) throws IOException {
    log.info("Reading pathway hierarchy from '{}'...", hierarchyFile);
    val document = readDocument(hierarchyFile);
    val root = document.getDocumentElement();

    val segments = new Stack<PathwaySegment>();
    val pathwayHierarchies = HashMultimap.<String, List<PathwaySegment>> create();

    readNode(root, segments, pathwayHierarchies);

    log.info("Finished reading {} pathway hierarchies", formatCount(pathwayHierarchies.size()));

    return pathwayHierarchies;
  }

  private static void readNode(Node node, Stack<PathwaySegment> segments,
      Multimap<String, List<PathwaySegment>> hierarchies) {
    val children = node.getChildNodes();

    for (int i = 0; i < children.getLength(); i++) {
      val child = children.item(i);

      if (isPathwayElement(child)) {
        val segment = convertPathwayNode(child);

        // Recurse
        segments.push(segment);

        // Keep track of path from root
        hierarchies.put(segment.getReactomeId(), ImmutableList.copyOf(segments));

        readNode(child, segments, hierarchies);
        segments.pop();
      }
    }
  }

  private static boolean isPathwayElement(Node currentNode) {
    return currentNode.getNodeType() == Node.ELEMENT_NODE
        && currentNode.getNodeName().equals(REACTOME_PATHWAY_ELEMENT_NAME);
  }

  private static Document readDocument(URI uri) throws IOException {
    return Documents.parseDocument(uri);
  }

}
