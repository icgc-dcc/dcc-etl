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
package org.icgc.dcc.etl.exporter.pig.udf;

import java.io.IOException;
import java.util.HashMap;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;
import org.icgc.dcc.downloader.core.ArchiveMetaManager;
import org.icgc.dcc.downloader.core.ArchiverConstant;
import org.icgc.dcc.downloader.core.SchemaUtil;

import com.google.common.base.Preconditions;

@Slf4j
public class CompositeKey extends EvalFunc<DataByteArray> {

  private static final long SEQUENCE_INTERVAL = 50000L;
  private final String dataType;
  private final String metaTablename;
  private final long interval;
  private final Configuration conf;
  private HashMap<Integer, Counter> counters = new HashMap<Integer, Counter>();
  private ArchiveMetaManager metaManager;

  @Data
  @AllArgsConstructor
  public class Counter {

    private long start;
    private long end;
  }

  public CompositeKey(String dataType, String releaseName, String sequenceInterval) throws IOException {
    super();
    this.dataType = dataType;
    if (releaseName == null || releaseName.trim().isEmpty()) {
      this.metaTablename = ArchiverConstant.META_TABLE_NAME;
    } else {
      this.metaTablename = SchemaUtil.getMetaTableName(releaseName);
    }

    this.interval = Long.valueOf(sequenceInterval);
    this.conf = UDFContext.getUDFContext().getJobConf();
    metaManager = new ArchiveMetaManager(metaTablename, conf);
  }

  public CompositeKey(String dataType, String releaseName) throws IOException {
    this(dataType, releaseName, String.valueOf(SEQUENCE_INTERVAL));
  }

  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    Integer donorId = (Integer) input.get(0);
    Counter counter = counters.get(donorId);
    if (counter == null || ((counter.end - counter.start) == 0L)) {
      long end = metaManager.getNextSequence(dataType, donorId, interval);
      long start = end - interval;
      counter = new Counter(start, end);
      counters.put(donorId, counter);
      log.debug("donor id: {}, start: {}, end: {}, interval: {}", donorId, start, end, interval);
    }
    return new DataByteArray(SchemaUtil.encodedArchiveRowKey(donorId, counter.start++));
  }

  @Override
  @SneakyThrows
  public Schema outputSchema(Schema inputSchema) {
    Preconditions.checkArgument(inputSchema.size() == 1);
    Preconditions.checkArgument(inputSchema.getField("donor_id") != null);
    Preconditions.checkArgument(inputSchema.getField("donor_id").type == DataType.INTEGER);
    return new Schema(new FieldSchema("rowkey", DataType.BYTEARRAY));
  }

  public static void init(String dataType, String releaseName) throws IOException {
    try (ArchiveMetaManager metaManager =
        new ArchiveMetaManager(SchemaUtil.getMetaTableName(releaseName), HBaseConfiguration.create())) {
      metaManager.deleteMetaInfo(dataType);
    }

  }
}
