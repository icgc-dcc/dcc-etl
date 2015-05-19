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
package org.icgc.dcc.etl.exporter.pig.storage;

import static org.icgc.dcc.downloader.core.ArchiverConstant.DATA_CONTENT_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.HEADER_SEPARATOR;

import java.io.IOException;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.icgc.dcc.downloader.core.ArchiveCompositeKey;
import org.icgc.dcc.downloader.core.ArchiveMetaManager;
import org.icgc.dcc.downloader.core.SchemaUtil;
//
// This can work. Note: 1. Need to run a total sort over the whole
// data set. 2. Need to sort the keyvalues before writing. It may be
// that simply calling ORDER with no other arguments in the pig script
// itself will be fine.
//

import com.google.common.base.Preconditions;

@Slf4j
public class DynamicStorage extends StoreFunc {

  protected RecordWriter writer = null;
  private final String tablename;
  private final String metaTablename;
  private String[] headers;
  private byte[] previousRowKey;
  private final String dataType;

  public DynamicStorage(String dataType, String releaseName)
  {
    Preconditions.checkArgument(dataType != null);
    Preconditions.checkArgument(releaseName != null && !releaseName.trim().isEmpty());

    this.tablename = SchemaUtil.getDataTableName(dataType, releaseName);
    this.dataType = dataType;
    this.metaTablename = SchemaUtil.getMetaTableName(releaseName);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public OutputFormat getOutputFormat() throws IOException {
    HFileOutputFormat2 outputFormat = new HFileOutputFormat2();
    return outputFormat;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    HFileOutputFormat2.setOutputPath(job, new Path(location));
  }

  public static Configuration getConfiguration(String tablename) {
    Configuration conf = new Configuration();
    HTableDescriptor schema = SchemaUtil.getDataTableSchema(tablename);
    conf.set("hbase.hfileoutputformat.families.compression",
        Bytes.toString(DATA_CONTENT_FAMILY) + "=" + schema.getFamily(DATA_CONTENT_FAMILY).getCompression().getName());
    conf.set("hbase.hfileoutputformat.families.bloomtype",
        Bytes.toString(DATA_CONTENT_FAMILY) + "=" + schema.getFamily(DATA_CONTENT_FAMILY).getBloomFilterType().name());
    conf.set("hbase.mapreduce.hfileoutputformat.blocksize",
        Bytes.toString(DATA_CONTENT_FAMILY) + "=" + schema.getFamily(DATA_CONTENT_FAMILY).getBlocksize());
    conf.set("hbase.mapreduce.hfileoutputformat.families.datablock.encoding",
        Bytes.toString(DATA_CONTENT_FAMILY) + "=" + schema.getFamily(DATA_CONTENT_FAMILY).getDataBlockEncoding().name());
    return conf;
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    log.info("Resource Schema: " + s.toString());
    // (donor_id:int, ranking:long, content:bag)
    Preconditions.checkArgument(s.getFields().length == 2);

    UDFContext udfc = UDFContext.getUDFContext();
    Properties p =
        udfc.getUDFProperties(this.getClass());
    StringBuilder sb = new StringBuilder();

    for (ResourceFieldSchema field : headerFields(s.getFields()[1])) {
      sb.append(field.getName());
      sb.append(HEADER_SEPARATOR);
    }
    sb.setLength(sb.length() - HEADER_SEPARATOR.length());
    String headerline = sb.toString();
    p.setProperty("headerline", headerline);
    log.info("header information: {}", headerline);

  }

  private ResourceFieldSchema[] headerFields(ResourceFieldSchema resourceFieldSchema) {
    Preconditions.checkArgument(resourceFieldSchema.getSchema().getFields().length == 1);
    ResourceFieldSchema tupleSchema = resourceFieldSchema.getSchema().getFields()[0];
    return tupleSchema.getSchema().getFields();
  }

  @Override
  public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
    this.writer = writer;
    Properties props = UDFContext.getUDFContext().getUDFProperties(
        this.getClass());
    String headerline = props.getProperty("headerline");
    Preconditions.checkNotNull(headerline);
    headers = headerline.split(HEADER_SEPARATOR);
    try (ArchiveMetaManager metaManager =
        new ArchiveMetaManager(metaTablename, UDFContext.getUDFContext().getJobConf())) {
      metaManager.addHeader(dataType, headers);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void putNext(Tuple t) throws IOException {

    DataByteArray key = (DataByteArray) t.get(0);
    byte[] rowKey = key.get();

    if (previousRowKey == null || (Bytes.compareTo(previousRowKey, rowKey) < 0)) {
      previousRowKey = rowKey;
    } else {
      ArchiveCompositeKey pRowKey = SchemaUtil.decodeArchiveRowKey(previousRowKey);
      ArchiveCompositeKey cRowKey = SchemaUtil.decodeArchiveRowKey(rowKey);
      log.error("Previous Row Key: (donor id: {}, ranking: {}), Current Row Key: (donor id: {}, ranking: {})",
          pRowKey.getDonorId(), pRowKey.getLineNum(), cRowKey.getDonorId(), cRowKey.getLineNum());
    }

    DataBag bag = (DataBag) t.get(1);
    Tuple content = bag.iterator().next();

    long now = System.currentTimeMillis();
    try {
      for (byte i = 0; i < headers.length; ++i) {
        Object cellValue = content.get(i);
        if (cellValue == null) continue;
        String value = (String) cellValue;
        if (value.trim().isEmpty()) continue;
        byte[] bytes = Bytes.toBytes(value);
        KeyValue kv = new KeyValue(rowKey, DATA_CONTENT_FAMILY,
            new byte[] { i }, now, bytes);
        writer.write(new ImmutableBytesWritable(rowKey), kv);
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted");
    }
  }
}