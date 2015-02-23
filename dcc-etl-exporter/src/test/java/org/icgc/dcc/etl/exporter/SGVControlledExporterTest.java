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

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import lombok.Cleanup;
import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.pig.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.icgc.dcc.downloader.core.ArchiveMetaManager;
import org.icgc.dcc.downloader.core.ArchiverConstant;
import org.icgc.dcc.etl.exporter.pig.storage.StaticMultiStorage;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.pigstats.ScriptState;

public class SGVControlledExporterTest extends EmbeddedDynamicExporter {

	final private static String LIB_PATH = StaticMultiStorage.class
			.getProtectionDomain().getCodeSource().getLocation().getPath();

	final String[] SGV_TSV_SCHEMA = { 
			"icgc_donor_id", //
			"project_code", //
			"icgc_specimen_id", //
			"icgc_sample_id", //
			"submitted_sample_id", //
			"analysis_id", //
			"chromosome", //
			"chromosome_start", //
			"chromosome_end", //
			"chromosome_strand", //
			"assembly_version", //
			"variant_type", //
			"reference_genome_allele", //
			"genotype", //
			"variant_allele", //
			"quality_score", //
			"probability", //
			"total_read_count", //
			"variant_allele_read_count", //
			"verification_status", //
			"verification_platform", //
			"consequence_type", //
			"aa_change", //
			"cds_change", //
			"gene_affected", //
			"transcript_affected", //
			"platform", //
			"experimental_protocol", //
			"base_calling_algorithm", //
			"alignment_algorithm", //
			"variation_calling_algorithm", //
			"other_analysis_algorithm", //
			"sequencing_strategy", //
			"seq_coverage", //
			"raw_data_repository", //
			"raw_data_accession", //
			"note" };

	final String[] SGV_JSON_SCHEMA = {
			"_donor_id", //
			"_project_id", //
			"_specimen_id", //
			"_sample_id", //
			"analyzed_sample_id",//
			"analysis_id",//
			"chromosome", "chromosome_start",//
			"chromosome_end",//
			"chromosome_strand", "assembly_version",//
			"variant_type",//
			"reference_genome_allele", "genotype",//
			"variant_allele",//
			"quality_score",//
			"probability", //
			"total_read_count",//
			"variant_allele_read_count", //
			"verification_status",//
			"verification_platform",//
			"consequence_type", //
			"aa_change", //
			"cds_change", //
			"gene_affected", //
			"transcript_affected", //
			"platform", //
			"experimental_protocol",//
			"base_calling_algorithm", // 
			"alignment_algorithm",//
			"variation_calling_algorithm", //
			"other_analysis_algorithm",//
			"sequencing_strategy",//
			"seq_coverage", // 
			"raw_data_repository",//
			"raw_data_accession",//
			"note" };

	final private static String DATA_TYPE= "sgv_controlled";

	@Test
	@SneakyThrows
	public void testDynamic() {
		exportToHBase(DATA_TYPE, "src/test/data/sgv_nc.json");
		Configuration conf = hbase.getConfiguration();
		@Cleanup
		ArchiveMetaManager archive = new ArchiveMetaManager(conf);
		List<String> header = archive.getHeader(DATA_TYPE);
		assertEquals(SGV_TSV_SCHEMA.length, header.size());
		for (String fieldname :SGV_TSV_SCHEMA) {
			assertTrue(header.contains(fieldname));
		}
		@Cleanup
		HTable table = new HTable(conf, DATA_TYPE);
		ResultScanner scan = table
				.getScanner(ArchiverConstant.DATA_CONTENT_FAMILY);
		Iterator<Result> itr = scan.iterator();
		long lines = 0;
		while (itr.hasNext()) {
			itr.next();
			++lines;
		}
		assertEquals(1, lines);
	}

	@Test
	public void sanity() throws IOException, ParseException,
			org.json.simple.parser.ParseException {
		PigServer pig = new PigServer(ExecType.LOCAL);
		pig.getPigContext()
				.getProperties()
				.setProperty("pig.import.search.path",
						LIB_PATH + "/sgv_controlled");
		Cluster cluster = new Cluster(pig.getPigContext());
		ScriptState.start("", pig.getPigContext());

		PigTest ssmTest = new PigTest(LIB_PATH + "/sgv_controlled/static.pig",
				new String[] { "OBSERVATION=src/test/data/sgv_nc.json",
						"LIB=" + LIB_PATH }, pig, cluster);

		JSONParser parser = new JSONParser();
		JSONObject json = (JSONObject) parser.parse(new FileReader(
				"src/test/data/sgv_nc.json"));

		ArrayList<Tuple> staticTuples = newArrayList(ssmTest
				.getAlias("static_out"));

		assertEquals("expect only 1 tuple in the result", 1,
				staticTuples.size());
		assertEquals("expect number columns", SGV_TSV_SCHEMA.length,
				staticTuples.get(0).size());

		for (int i = 0; i < staticTuples.get(0).size(); ++i) {
			Object field = staticTuples.get(0).get(i);
			String fieldValue = field == null ? null : (field.equals("") ? null
					: (String) field);

			Object result = json.get(SGV_JSON_SCHEMA[i]);
			String expectedValue = result == null ? null : String
					.valueOf(result);
			assertEquals("Field: " + SGV_JSON_SCHEMA[i], expectedValue,
					fieldValue);
		}

	}
}
