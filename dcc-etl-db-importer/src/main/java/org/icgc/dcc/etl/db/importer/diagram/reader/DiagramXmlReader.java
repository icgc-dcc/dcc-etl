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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.icgc.dcc.etl.db.importer.diagram.reader.DiagramReader.REACTOME_BASE_URL;

import java.net.URL;

import lombok.NonNull;
import lombok.val;

import com.google.common.io.Resources;

public class DiagramXmlReader {

  private final static String DIAGRAM_XML_URL = REACTOME_BASE_URL + "pathwayDiagram/%s/XML";

  public String readPathwayXml(@NonNull String dbId) throws Exception {
    val url = new URL(format(DIAGRAM_XML_URL, dbId));
    return escape(Resources.toString(url, UTF_8));
  }

  private String escape(String xml) {
    for (String[] replacement : replacements) {
      xml.replace(replacement[0], replacement[1]);
    }
    return xml;
  }

  private String[][] replacements =
  {
      { "\b", "\\b" },
      { "\n", "\\n" },
      { "\t", "\\t" },
      { "\f", "\\f" },
      { "\r", "\\r" },
      { "\"", "\\\"" },
      { "\\", "\\\\" },
      { "/", "\\/" }
  };

}
