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
package org.icgc.dcc.etl.exporter;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.icgc.dcc.etl.exporter.pig.udf.DataDownloadStorage;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class SGVControlledExporterTest {

  final private static String LIB_PATH = DataDownloadStorage.class.getProtectionDomain().getCodeSource().getLocation()
      .getPath();

  final String[] SGV_TSV_SCHEMA =
  {
      "icgc_variant_id",
      "icgc_donor_id",
      "project_code",
      "icgc_specimen_id",
      "icgc_sample_id",
      "submitted_sample_id",
      "analysis_id",
      "chromosome",
      "chromosome_start",
      "chromosome_end",
      "chromosome_strand",
      "assembly_version",
      "variant_type",
      "reference_genome_allele",
      "genotype",
      "variant_allele",
      "quality_score",
      "probability",
      "total_read_count",
      "variant_allele_read_count",
      "verification_status",
      "verification_platform",
      "platform",
      "experimental_protocol",
      "base_calling_algorithm",
      "alignment_algorithm",
      "variation_calling_algorithm",
      "other_analysis_algorithm",
      "sequencing_strategy",
      "seq_coverage",
      "raw_data_repository",
      "raw_data_accession",
      "note"
  };

  final String[] SGV_JSON_SCHEMA =
  {
      "_variant_id",
      "_donor_id",
      "_project_id",
      "_specimen_id",
      "_sample_id",
      "analyzed_sample_id",
      "analysis_id",
      "chromosome",
      "chromosome_start",
      "chromosome_end",
      "chromosome_strand",
      "assembly_version",
      "variant_type",
      "reference_genome_allele",
      "genotype",
      "variant_allele",
      "quality_score",
      "probability",
      "total_read_count",
      "variant_allele_read_count",
      "verification_status",
      "verification_platform",
      "platform",
      "experimental_protocol",
      "base_calling_algorithm",
      "alignment_algorithm",
      "variation_calling_algorithm",
      "other_analysis_algorithm",
      "sequencing_strategy",
      "seq_coverage",
      "raw_data_repository",
      "raw_data_accession",
      "note"
  };

  @Test
  public void sanity() throws IOException, ParseException, org.json.simple.parser.ParseException {
    PigTest ssmTest =
        new PigTest("src/main/pig/scripts/sgvexporter.pig",
            new String[] { "OBSERVATION=src/test/data/sgv_nc.json", "LIB="
                + LIB_PATH });

    JSONParser parser = new JSONParser();
    JSONObject json = (JSONObject) parser.parse(new FileReader("src/test/data/sgv_nc.json"));

    ArrayList<Tuple> staticTuples = newArrayList(ssmTest.getAlias("static_out"));

    assertEquals("expect only 1 tuple in the result", 1, staticTuples.size());
    assertEquals("must be key-value pair", 2, staticTuples.get(0).size());
    DefaultDataBag staticBag = (DefaultDataBag) staticTuples.get(0).get(1);
    ArrayList<Tuple> staticContent = newArrayList(staticBag.iterator());
    assertEquals("expect only 1 tuple in the result", 1, staticContent.size());
    assertEquals("expect number columns", SGV_TSV_SCHEMA.length, staticContent.get(0).size());

    for (int i = 0; i < staticContent.get(0).size(); ++i) {
      Object field = staticContent.get(0).get(i);
      String fieldValue =
          field == null ? null : (field.equals("") ? null : (String) field);

      Object result = json.get(SGV_JSON_SCHEMA[i]);
      String expectedValue =
          result == null ? null : String.valueOf(result);
      assertEquals("Field: " + SGV_JSON_SCHEMA[i], expectedValue, fieldValue);
    }

  }
}
