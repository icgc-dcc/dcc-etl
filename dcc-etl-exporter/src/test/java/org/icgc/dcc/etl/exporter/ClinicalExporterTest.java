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
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.icgc.dcc.etl.exporter.pig.udf.DataDownloadStorage;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ClinicalExporterTest {

  final private static String LIB_PATH = DataDownloadStorage.class.getProtectionDomain().getCodeSource().getLocation()
      .getPath();

  final private static String EMPTY_VALUE = "NO_VAL";
  final String[] CLINICAL_TSV_SCHEMA =
  {
      "icgc_donor_id",
      "project_code",
      "submitted_donor_id",
      "donor_sex",
      "donor_vital_status",
      "disease_status_last_followup",
      "donor_relapse_type",
      "donor_age_at_diagnosis",
      "donor_age_at_enrollment",
      "donor_age_at_last_followup",
      "donor_relapse_interval",
      "donor_diagnosis_icd10",
      "donor_tumour_staging_system_at_diagnosis",
      "donor_tumour_stage_at_diagnosis",
      "donor_tumour_stage_at_diagnosis_supplemental",
      "donor_survival_time",
      "donor_interval_of_last_followup",
      "icgc_specimen_id",
      "submitted_specimen_id",
      "specimen_type",
      "specimen_type_other",
      "specimen_interval",
      "specimen_donor_treatment_type",
      "specimen_donor_treatment_type_other",
      "specimen_processing",
      "specimen_processing_other",
      "specimen_storage",
      "specimen_storage_other",
      "tumour_confirmed",
      "specimen_biobank",
      "specimen_biobank_id",
      "specimen_available",
      "tumour_histological_type",
      "tumour_grading_system",
      "tumour_grade",
      "tumour_grade_supplemental",
      "tumour_stage_system",
      "tumour_stage",
      "tumour_stage_supplemental",
      "digital_image_of_stained_section"
  };

  final List<String> SPECIMENS = newArrayList(
      "_specimen_id",
      "specimen_id",
      "specimen_type",
      "specimen_type_other",
      "specimen_interval",
      "specimen_donor_treatment_type",
      "specimen_donor_treatment_type_other",
      "specimen_processing",
      "specimen_processing_other",
      "specimen_storage",
      "specimen_storage_other",
      "tumour_confirmed",
      "specimen_biobank",
      "specimen_biobank_id",
      "specimen_available",
      "tumour_histological_type",
      "tumour_grading_system",
      "tumour_grade",
      "tumour_grade_supplemental",
      "tumour_stage_system",
      "tumour_stage",
      "tumour_stage_supplemental",
      "digital_image_of_stained_section"
      );

  final String[] CLINICAL_JSON_SCHEMA =
  {
      "_donor_id",
      "_project_id",
      "donor_id",
      "donor_sex",
      "donor_vital_status",
      "disease_status_last_followup",
      "donor_relapse_type",
      "donor_age_at_diagnosis",
      "donor_age_at_enrollment",
      "donor_age_at_last_followup",
      "donor_relapse_interval",
      "donor_diagnosis_icd10",
      "donor_tumour_staging_system_at_diagnosis",
      "donor_tumour_stage_at_diagnosis",
      "donor_tumour_stage_at_diagnosis_supplemental",
      "donor_survival_time",
      "donor_interval_of_last_followup",
      "_specimen_id",
      "specimen_id",
      "specimen_type",
      "specimen_type_other",
      "specimen_interval",
      "specimen_donor_treatment_type",
      "specimen_donor_treatment_type_other",
      "specimen_processing",
      "specimen_processing_other",
      "specimen_storage",
      "specimen_storage_other",
      "tumour_confirmed",
      "specimen_biobank",
      "specimen_biobank_id",
      "specimen_available",
      "tumour_histological_type",
      "tumour_grading_system",
      "tumour_grade",
      "tumour_grade_supplemental",
      "tumour_stage_system",
      "tumour_stage",
      "tumour_stage_supplemental",
      "digital_image_of_stained_section"
  };

  @Test
  public void basicNoSpecimenTest() throws IOException, ParseException, org.json.simple.parser.ParseException {
    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.getPigContext()
        .getProperties()
        .setProperty("pig.import.search.path",
            "/Users/JLam/git/dcc/dcc-downloader/src/main/scripts/pig/clinical");
    Cluster cluster = new Cluster(pig.getPigContext());
    PigTest ssmTest =
        new PigTest("src/main/scripts/pig/clinical/static.pig",
            new String[] {
                "OBSERVATION=src/test/data/clinical_nc.json",
                "LIB=" + LIB_PATH,
                "EMPTY_VALUE=" + EMPTY_VALUE
            },
            pig,
            cluster);

    JSONParser parser = new JSONParser();
    JSONObject json = (JSONObject) parser.parse(new FileReader("src/test/data/clinical_nc.json"));

    ArrayList<Tuple> staticTuples = newArrayList(ssmTest.getAlias("static_out"));

    assertEquals("expect only 1 tuple in the result", 1, staticTuples.size());
    assertEquals("must be key-value pair", 2, staticTuples.get(0).size());
    DefaultDataBag staticBag = (DefaultDataBag) staticTuples.get(0).get(1);
    ArrayList<Tuple> staticContent = newArrayList(staticBag.iterator());
    assertEquals("expect only 1 tuple in the result", 1, staticContent.size());
    assertEquals("expect number columns", CLINICAL_JSON_SCHEMA.length, staticContent.get(0).size());

    for (int i = 0; i < staticContent.get(0).size(); ++i) {
      Object field = staticContent.get(0).get(i);
      String fieldValue =
          field == null ? EMPTY_VALUE : (field.equals("") ? EMPTY_VALUE : (String) field);

      Object result = null;
      if (!SPECIMENS.contains(CLINICAL_JSON_SCHEMA[i])) {
        result = json.get(CLINICAL_JSON_SCHEMA[i]);
      }
      String expectedValue =
          result == null ? EMPTY_VALUE : String.valueOf(result);
      assertEquals("Field: " + CLINICAL_JSON_SCHEMA[i], expectedValue, fieldValue);
    }

  }

  @Test
  public void basicMultiTest() throws IOException, ParseException, org.json.simple.parser.ParseException {
    PigTest ssmTest =
        new PigTest("src/main/pig/scripts/clinicalexporter.pig",
            new String[] {
                "OBSERVATION=src/test/data/clinical_mc.json",
                "LIB=" + LIB_PATH,
                "EMPTY_VALUE=" + EMPTY_VALUE
            });

    JSONParser parser = new JSONParser();
    JSONObject json = (JSONObject) parser.parse(new FileReader("src/test/data/clinical_mc.json"));

    ArrayList<Tuple> staticTuples = newArrayList(ssmTest.getAlias("static_out"));

    assertEquals("expect only 1 tuple in the result", 1, staticTuples.size());
    assertEquals("must be key-value pair", 2, staticTuples.get(0).size());
    DefaultDataBag staticBag = (DefaultDataBag) staticTuples.get(0).get(1);
    ArrayList<Tuple> staticContent = newArrayList(staticBag.iterator());
    assertEquals("expect 2 tuples in the result", 2, staticContent.size());
    assertEquals("expect number columns", CLINICAL_TSV_SCHEMA.length, staticContent.get(0).size());
    assertEquals("expect number columns", CLINICAL_TSV_SCHEMA.length, staticContent.get(1).size());

    JSONArray specimens = ((JSONArray) json.get("specimen"));
    for (int i = 0; i < specimens.size(); ++i) {
      JSONObject specimen = (JSONObject) specimens.get(i);
      Tuple tuple = staticContent.get(i);
      for (int j = 0; j < tuple.size(); ++j) {
        Object field = tuple.get(j);
        String fieldValue =
            field == null ? EMPTY_VALUE : (field.equals("") ? EMPTY_VALUE : (String) field);
        Object result = null;
        if (SPECIMENS.contains(CLINICAL_JSON_SCHEMA[j])) {
          result = specimen.get(CLINICAL_JSON_SCHEMA[j]);
        } else {
          result = json.get(CLINICAL_JSON_SCHEMA[j]);

        }
        String expectedValue =
            result == null ? EMPTY_VALUE : String.valueOf(result);
        assertEquals("Field: " + CLINICAL_JSON_SCHEMA[j], expectedValue, fieldValue);

      }

    }

  }
}
