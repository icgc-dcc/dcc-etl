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
package org.icgc.dcc.etl.db.importer.repo.cghub.reader;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.repo.cghub.reader.CGHubAnalysisDetailReader;
import org.junit.Test;

@Slf4j
public class CGHubAnalysisDetailReaderTest {

  @Test
  public void testReadDetails() throws Exception {
    val reader = new CGHubAnalysisDetailReader();

    val diseaseCode = "UCEC";
    val details = reader.readDetails(diseaseCode);

    val results = details.get("result_set").get("results");
    for (val result : results) {
      log.info("----------------------------------");
      log.info("sample_id: {}", result.get("sample_id"));
      log.info("legacy_sample_id: {}", result.get("legacy_sample_id"));
      log.info("analyte_code: {}", result.get("analyte_code"));
      log.info("sample_type: {}", result.get("sample_type"));
      log.info("platform: {}", result.get("platform"));
      log.info("aliquot_id: {}", result.get("aliquot_id"));
      log.info("participant_id: {}", result.get("participant_id"));
      log.info("library_strategy: {}", result.get("library_strategy"));
      log.info("last_modified: {}", result.get("last_modified"));

      val files = result.get("files");
      for (val file : files) {
        // TODO: Parse filesize, filename; ignore '*.bam.bai'
        log.info("file: {}", file);
      }
    }
  }

}
