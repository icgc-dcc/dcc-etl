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

import static com.google.common.collect.Maps.newHashMap;
import static java.lang.String.format;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import lombok.val;

import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONTokener;

public class DiagramProteinMapReader {

  private final String PROTEIN_MAP_URL =
      "http://reactomews.oicr.on.ca:8080/ReactomeRESTfulAPI/RESTfulWS/getPhysicalToReferenceEntityMaps/%s";

  public Map<String, String> readProteinMap(String pathwayId) throws IOException, JSONException {
    val result = (JSONArray) new JSONTokener(IOUtils.toString(new URL(format(PROTEIN_MAP_URL, pathwayId)))).nextValue();
    Map<String, String> proteinMap = newHashMap();

    for (int i = 0; i < result.length(); i++) {
      val entities = result.getJSONObject(i).getJSONArray("refEntities");
      val dbId = result.getJSONObject(i).getString("peDbId");

      String referenceIds = "";
      for (int j = 0; j < entities.length(); j++) {
        val entity = entities.getJSONObject(j);

        if (entity.getString("schemaClass").equalsIgnoreCase("ReferenceGeneProduct")) {
          referenceIds += entity.getString("displayName")
              .substring(0, entity.getString("displayName").indexOf(" "));

          if (j < entities.length() - 1) {
            referenceIds += ",";
          }
        }

      }

      if (!referenceIds.isEmpty()) {
        proteinMap.put(dbId, referenceIds);
      }
    }

    return proteinMap;
  }

}