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
package org.icgc.dcc.etl.indexer.hadoop;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.test.json.JsonNodes.$;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import lombok.SneakyThrows;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cascading.CascadingTestCase;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TupleOutputStream;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Tests {@code JsonNodeSerialization}.
 */
public class ObjectNodeSerializationTest extends CascadingTestCase {

  @Rule
  TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testSerde() throws IOException {
    // Setup
    TupleSerialization tupleSerialization = new TupleSerialization(createJobConf());
    File file = tmp.newFile();
    System.out.println(file);

    // @formatter:off
    // Test data
    JsonNode a = $(
      "{a:1}"
    );
    JsonNode b = $(
      "{b:2}"
    );    
    JsonNode c = $(
      "{c:3}"
    );    
    // @formatter:on

    // Output tuple serialization
    List<Tuple> outputTuples = writeTuples(tupleSerialization, file, a, b, c);

    // Input for tuple serialization
    List<Tuple> inputTuples = readTuples(tupleSerialization, file, 3);

    // Check the first item in the tuple is a TupleEntry object
    assertThat(inputTuples.get(0).getObject(0)).isInstanceOf(JsonNode.class);
    assertThat(inputTuples.get(0).toString()).isEqualTo(outputTuples.get(0).toString());
  }

  @SneakyThrows
  private List<Tuple> writeTuples(TupleSerialization tupleSerialization, File file, JsonNode... jsonNodes) {
    TupleOutputStream output =
        new HadoopTupleOutputStream(new FileOutputStream(file, false), tupleSerialization.getElementWriter());

    List<Tuple> outputTuples = newArrayList();
    for (JsonNode jsonNode : jsonNodes) {
      Tuple outputTuple = new Tuple(jsonNode);
      output.writeTuple(outputTuple);

      outputTuples.add(outputTuple);
    }
    output.close();

    return outputTuples;
  }

  @SneakyThrows
  private List<Tuple> readTuples(TupleSerialization tupleSerialization, File file, int count) {
    TupleInputStream input =
        new HadoopTupleInputStream(new FileInputStream(file), tupleSerialization.getElementReader());

    List<Tuple> inputTuples = newArrayList();
    for (int i = 0; i < count; i++) {
      inputTuples.add(input.readTuple());
    }
    input.close();

    return inputTuples;
  }

  private JobConf createJobConf() {
    JobConf jobConf = new JobConf();
    jobConf.set("io.serializations", ObjectNodeSerialization.class.getName());

    return jobConf;
  }

}
