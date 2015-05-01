/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.db.importer.cghub.core;

import static org.icgc.dcc.etl.db.importer.cghub.util.CGHubMetadata.CGHUB_BASE_URL;
import static org.icgc.dcc.etl.db.importer.file.util.FileRepositories.FILE_REPOSITORY_CGHUB_VALUE;
import static org.icgc.dcc.etl.db.importer.file.util.FileRepositories.FILE_REPOSITORY_FIELD_NAME;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class CGHubAnalysisDetailProcessor {

  public Iterable<ObjectNode> process(JsonNode details) {
    val cghubFiles = ImmutableList.<ObjectNode> builder();

    val results = details.path("result_set").path("results");
    for (val result : results) {
      val files = result.get("files");
      for (val file : files) {
        if (isBaiFile(file)) {
          continue;
        }

        val cghubFile = createCGHubFile(result, (ObjectNode) file);

        cghubFiles.add(cghubFile);
      }
    }

    return cghubFiles.build();
  }

  private static ObjectNode createCGHubFile(JsonNode result, ObjectNode file) {
    val cghubFile = file.objectNode();
    cghubFile.put(FILE_REPOSITORY_FIELD_NAME, FILE_REPOSITORY_CGHUB_VALUE);
    cghubFile.put("gnos_id", result.get("analysis_id"));
    cghubFile.putPOJO("gnos_url", ImmutableList.of(CGHUB_BASE_URL));
    cghubFile.put("filesize", file.get("filesize").longValue());
    cghubFile.put("filename", file.get("filename"));
    cghubFile.put("sample_id", result.get("sample_id"));
    cghubFile.put("legacy_sample_id", result.get("legacy_sample_id"));
    cghubFile.put("analyte_code", result.get("analyte_code"));
    cghubFile.put("sample_type", result.get("sample_type"));
    cghubFile.put("platform", result.get("platform"));
    cghubFile.put("aliquot_id", result.get("aliquot_id"));
    cghubFile.put("participant_id", result.get("participant_id"));
    cghubFile.put("library_strategy", result.get("library_strategy"));
    cghubFile.put("last_modified", result.get("last_modified"));

    return cghubFile;
  }

  private static boolean isBaiFile(JsonNode file) {
    val fileName = file.get("filename").textValue();

    return fileName.endsWith(".bam.bai");
  }

}
