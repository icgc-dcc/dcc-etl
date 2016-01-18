/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.icgc.dcc.etl.loader.cascading;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TupleOutputStream;

/**
 * Tests {@code TupleEntrySerialization}.
 */
public class TupleEntrySerializationTest extends CascadingTestCase {

  @Rule
  TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void test_tuple_entry_serialization() throws IOException {
    // Setup
    TupleSerialization tupleSerialization = new TupleSerialization(createJobConf());
    File file = tmp.newFile();

    // @formatter:off
    TupleEntry testEntry = 
      new TupleEntry(
        new Fields("x", "y", "z"), 
        new Tuple(
          new TupleEntry(
            new Fields("n1", "n2"),
            new Tuple(4, 5)
          ), // x
          2, // y
          3  // z
        )
      );
    // @formatter:on

    // Output tuple state serialization
    Tuple outputTuple = writeTuple(tupleSerialization, file, testEntry);

    // Input for tuple state serialization
    Tuple inputTuple = readTuple(tupleSerialization, file);

    // Check the first item in the tuple is a TupleEntry object
    assertThat(inputTuple.getObject(0)).isInstanceOf(TupleEntry.class);
    assertThat(inputTuple.toString()).isEqualTo(outputTuple.toString());
  }

  private Tuple writeTuple(TupleSerialization tupleSerialization, File file, TupleEntry tupleEntry)
      throws FileNotFoundException, IOException {
    TupleOutputStream output =
        new HadoopTupleOutputStream(new FileOutputStream(file, false), tupleSerialization.getElementWriter());
    Tuple outputTuple = new Tuple(tupleEntry);
    output.writeTuple(outputTuple);
    output.close();

    return outputTuple;
  }

  private Tuple readTuple(TupleSerialization tupleSerialization, File file) throws FileNotFoundException, IOException {
    TupleInputStream input =
        new HadoopTupleInputStream(new FileInputStream(file), tupleSerialization.getElementReader());
    Tuple inputTuple = input.readTuple();
    input.close();

    return inputTuple;
  }

  private JobConf createJobConf() {
    JobConf jobConf = new JobConf();
    jobConf.set("io.serializations", TupleEntrySerialization.class.getName());

    return jobConf;
  }

}
