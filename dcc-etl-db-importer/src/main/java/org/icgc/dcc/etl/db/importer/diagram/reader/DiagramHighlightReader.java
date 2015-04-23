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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.readLines;
import static org.icgc.dcc.etl.db.importer.diagram.reader.DiagramReader.REACTOME_BASE_URL;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DiagramHighlightReader {

  public static final String CONTAINED_EVENTS_URL = REACTOME_BASE_URL + "getContainedEventIds/%s";
  private static final List<String> FAILURES = new ArrayList<String>();

  public String readHighlights(@NonNull String pathwayId) throws IOException {
    val diagramUrl = new URL(String.format(CONTAINED_EVENTS_URL, pathwayId));

    val connection = (HttpURLConnection) diagramUrl.openConnection();
    if (connection.getResponseCode() == 500) {
      log.error("500 Server Error from Reactome!\nFailed to get URL: {}\nSetting '{}' highlights to \"\"",
          diagramUrl.toExternalForm(), pathwayId);
      FAILURES.add(pathwayId);
      return "";
    }

    // Read the first (and only) line that contains a list of reaction ids to zoom in on
    return readLines(diagramUrl, UTF_8).get(0);
  }

  public static List<String> getFailedPathways() {
    return FAILURES;
  }
}
