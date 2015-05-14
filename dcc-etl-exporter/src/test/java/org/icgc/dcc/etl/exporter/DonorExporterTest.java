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
package org.icgc.dcc.etl.exporter;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.Cleanup;
import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.pig.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.apache.pig.tools.pigstats.ScriptState;
import org.icgc.dcc.downloader.core.ArchiveMetaManager;
import org.icgc.dcc.downloader.core.ArchiverConstant;
import org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class DonorExporterTest extends EmbeddedDynamicExporter {

  final private static String LIB_PATH = StaticMultiStorage.class.getProtectionDomain().getCodeSource().getLocation()
      .getPath();

  final private static String EMPTY_VALUE = "NO_VAL";
  final String[] DONOR_TSV_SCHEMA =
      { "icgc_donor_id", "project_code", "study_donor_involved_in", "submitted_donor_id", "donor_sex", "donor_vital_status", "disease_status_last_followup", "donor_relapse_type", "donor_age_at_diagnosis", "donor_age_at_enrollment", "donor_age_at_last_followup", "donor_relapse_interval", "donor_diagnosis_icd10", "donor_tumour_staging_system_at_diagnosis", "donor_tumour_stage_at_diagnosis", "donor_tumour_stage_at_diagnosis_supplemental", "donor_survival_time", "donor_interval_of_last_followup", "prior_malignancy", "cancer_type_prior_malignancy", "cancer_history_first_degree_relative" };

  final List<String> DONORS = newArrayList("exposure_intensity", "exposure_type");

  final String[] DONOR_JSON_SCHEMA =
      { "_donor_id", "_project_id", "study_donor_involved_in", "donor_id", "donor_sex", "donor_vital_status", "disease_status_last_followup", "donor_relapse_type", "donor_age_at_diagnosis", "donor_age_at_enrollment", "donor_age_at_last_followup", "donor_relapse_interval", "donor_diagnosis_icd10", "donor_tumour_staging_system_at_diagnosis", "donor_tumour_stage_at_diagnosis", "donor_tumour_stage_at_diagnosis_supplemental", "donor_survival_time", "donor_interval_of_last_followup", "prior_malignancy", "cancer_type_prior_malignancy", "cancer_history_first_degree_relative" };

  final private static String DATA_TYPE = "donor";

  @Test
  @SneakyThrows
  public void testDynamic() {
    exportToHBase(DATA_TYPE, "src/test/data/clinical_mc.json");
    Configuration conf = hbase.getConfiguration();
    @Cleanup
    ArchiveMetaManager archive = new ArchiveMetaManager(conf);
    List<String> header = archive.getHeader(DATA_TYPE);
    assertEquals(DONOR_TSV_SCHEMA.length, header.size());
    for (String fieldname : DONOR_TSV_SCHEMA) {
      assertTrue(header.contains(fieldname));
    }
    @Cleanup
    HTable table = new HTable(conf, DATA_TYPE);
    ResultScanner scan = table.getScanner(ArchiverConstant.DATA_CONTENT_FAMILY);
    long lines = Iterators.size(scan.iterator());
    assertEquals(1, lines);
  }

  @Test
  public void sanity() throws IOException, ParseException, org.json.simple.parser.ParseException {

    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.getPigContext().getProperties().setProperty("pig.import.search.path", LIB_PATH + "/" + DATA_TYPE);
    Cluster cluster = new Cluster(pig.getPigContext());
    ScriptState.start("", pig.getPigContext());
    PigTest ssmTest =
        new PigTest(LIB_PATH + "/" + DATA_TYPE + "/static.pig",
            new String[] { "OBSERVATION=src/test/data/clinical_nc.json", "LIB=" + LIB_PATH, "EMPTY_VALUE="
                + EMPTY_VALUE, "DEFAULT_PARALLEL=1" }, pig, cluster);

    JSONParser parser = new JSONParser();
    JSONObject json = (JSONObject) parser.parse(new FileReader("src/test/data/clinical_nc.json"));

    ArrayList<Tuple> staticTuples = newArrayList(ssmTest.getAlias("static_out"));

    assertEquals("expect only 1 tuple in the result", 1, staticTuples.size());
    assertEquals("expect number columns", DONOR_JSON_SCHEMA.length, staticTuples.get(0).size());

    for (int i = 0; i < staticTuples.get(0).size(); ++i) {
      Object field = staticTuples.get(0).get(i);
      String fieldValue = field == null ? EMPTY_VALUE : (field.equals("") ? EMPTY_VALUE : (String) field);

      Object result = json.get(DONOR_JSON_SCHEMA[i]);
      String expectedValue = result == null ? EMPTY_VALUE : String.valueOf(result);
      assertEquals("Field: " + DONOR_JSON_SCHEMA[i], expectedValue, fieldValue);
    }

  }

}
