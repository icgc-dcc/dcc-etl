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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class DonorTherapyExporterTest extends EmbeddedDynamicExporter {

  final private static String LIB_PATH = StaticMultiStorage.class.getProtectionDomain().getCodeSource().getLocation()
      .getPath();

  final private static String EMPTY_VALUE = "NO_VAL";
  final String[] THERAPY_TSV_SCHEMA =
      { "icgc_donor_id", "project_code", "submitted_donor_id", "first_therapy_type", "first_therapy_therapeutic_intent", "first_therapy_start_interval", "first_therapy_duration", "first_therapy_response", "second_therapy_type", "second_therapy_therapeutic_intent", "second_therapy_start_interval", "second_therapy_duration", "second_therapy_response", "other_therapy", "other_therapy_response" };

  final List<String> THERAPIES = newArrayList("therapy_response", "therapy_start_interval", "therapy_type");

  final String[] THERAPY_JSON_SCHEMA =
      { "_donor_id", "_project_id", "donor_id", "first_therapy_type", "first_therapy_therapeutic_intent", "first_therapy_start_interval", "first_therapy_duration", "first_therapy_response", "second_therapy_type", "second_therapy_therapeutic_intent", "second_therapy_start_interval", "second_therapy_duration", "second_therapy_response", "other_therapy", "other_therapy_response" };

  final private static String DATA_TYPE = "donor_therapy";

  @Test
  @SneakyThrows
  public void testDynamic() {
    exportToHBase(DATA_TYPE, "src/test/data/clinical_mc.json");
    Configuration conf = hbase.getConfiguration();
    @Cleanup
    ArchiveMetaManager archive = new ArchiveMetaManager(conf);
    List<String> header = archive.getHeader(DATA_TYPE);
    assertEquals(THERAPY_TSV_SCHEMA.length, header.size());
    for (String fieldname : THERAPY_TSV_SCHEMA) {
      assertTrue(header.contains(fieldname));
    }
    @Cleanup
    HTable table = new HTable(conf, DATA_TYPE);
    ResultScanner scan = table.getScanner(ArchiverConstant.DATA_CONTENT_FAMILY);
    long lines = Iterators.size(scan.iterator());
    assertEquals(2, lines);
  }

  @Test
  public void basicNoFamilyTest() throws IOException, ParseException, org.json.simple.parser.ParseException {

    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.getPigContext().getProperties().setProperty("pig.import.search.path", LIB_PATH + "/" + DATA_TYPE);
    Cluster cluster = new Cluster(pig.getPigContext());
    ScriptState.start("", pig.getPigContext());
    PigTest ssmTest =
        new PigTest(LIB_PATH + "/" + DATA_TYPE + "/static.pig",
            new String[] { "OBSERVATION=src/test/data/clinical_nc.json", "LIB=" + LIB_PATH, "EMPTY_VALUE="
                + EMPTY_VALUE, "DEFAULT_PARALLEL=1" }, pig, cluster);
    ArrayList<Tuple> staticTuples = newArrayList(ssmTest.getAlias("static_out"));
    assertEquals("expect no tuple in the result", 0, staticTuples.size());

  }

  @Test
  public void basicMultiFamilyTest() throws IOException, ParseException, org.json.simple.parser.ParseException {
    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.getPigContext().getProperties().setProperty("pig.import.search.path", LIB_PATH + "/" + DATA_TYPE);
    Cluster cluster = new Cluster(pig.getPigContext());
    ScriptState.start("", pig.getPigContext());

    PigTest ssmTest =
        new PigTest(LIB_PATH + "/" + DATA_TYPE + "/static.pig",
            new String[] { "OBSERVATION=src/test/data/clinical_mc.json", "LIB=" + LIB_PATH, "EMPTY_VALUE="
                + EMPTY_VALUE, "DEFAULT_PARALLEL=1" }, pig, cluster);

    JSONParser parser = new JSONParser();
    JSONObject json = (JSONObject) parser.parse(new FileReader("src/test/data/clinical_mc.json"));

    ArrayList<Tuple> staticTuples = newArrayList(ssmTest.getAlias("static_out"));

    assertEquals("expect 2 tuples in the result", 2, staticTuples.size());
    assertEquals("expect number columns", THERAPY_TSV_SCHEMA.length, staticTuples.get(0).size());
    assertEquals("expect number columns", THERAPY_TSV_SCHEMA.length, staticTuples.get(1).size());

    JSONArray therapies = ((JSONArray) json.get("therapy"));
    for (int i = 0; i < therapies.size(); ++i) {
      JSONObject therapy = (JSONObject) therapies.get(i);
      Tuple tuple = staticTuples.get(i);
      for (int j = 0; j < tuple.size(); ++j) {
        Object field = tuple.get(j);
        String fieldValue = (field == null) ? EMPTY_VALUE : (field.equals("") ? EMPTY_VALUE : (String) field);
        Object result = null;
        if (THERAPIES.contains(THERAPY_JSON_SCHEMA[j])) {
          result = therapy.get(THERAPY_JSON_SCHEMA[j]);
        } else {
          result = json.get(THERAPY_JSON_SCHEMA[j]);

        }
        String expectedValue = result == null ? EMPTY_VALUE : String.valueOf(result);
        assertEquals("Field: " + THERAPY_JSON_SCHEMA[j], expectedValue, fieldValue);

      }

    }

  }

}
