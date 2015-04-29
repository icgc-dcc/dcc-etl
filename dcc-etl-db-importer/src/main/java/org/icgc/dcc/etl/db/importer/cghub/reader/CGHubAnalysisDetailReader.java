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
package org.icgc.dcc.etl.db.importer.cghub.reader;

import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static org.icgc.dcc.common.core.util.Jackson.DEFAULT;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public class CGHubAnalysisDetailReader {

  /**
   * Constants.
   */
  private static final String CGHUB_BASE_URL = "https://cghub.ucsc.edu";
  private static final String CGHUB_API_URL = CGHUB_BASE_URL + "/cghub/metadata";
  private static final String CGHUB_ANALYSIS_DETAIL_API_URL = CGHUB_API_URL + "/analysisDetail";

  @SneakyThrows
  public ObjectNode readDetails(String diseaseCode) {
    val watch = createStarted();
    val url = createUrl(diseaseCode);

    log.info("Reading analysis details for disease code '{}' from '{}'... ", diseaseCode, url);
    @Cleanup
    val inputStream = openInputStream(url);
    val details = readDetails(inputStream);
    log.info("Finished eading analysis details for disease code '{}' in {} ", diseaseCode, watch);

    return details;
  }

  private static ObjectNode readDetails(InputStream inputStream) throws IOException, JsonProcessingException {
    return (ObjectNode) DEFAULT.readTree(inputStream);
  }

  private static URL createUrl(String diseaseCode) throws MalformedURLException {
    val state = "live"; // Per Junjun
    val params = "disease_abbr" + "=" + diseaseCode + "&" + "state" + "=" + state;

    return new URL(CGHUB_ANALYSIS_DETAIL_API_URL + "?" + params);
  }

  private static InputStream openInputStream(URL url) throws IOException {
    val connection = (HttpURLConnection) url.openConnection();
    connection.setRequestProperty(ACCEPT, "application/json");

    return connection.getInputStream();
  }

}
