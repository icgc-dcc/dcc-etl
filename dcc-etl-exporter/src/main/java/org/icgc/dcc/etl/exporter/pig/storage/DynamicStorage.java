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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.icgc.dcc.downloader.core.ArchiveMetaManager;
import org.icgc.dcc.downloader.core.ArchiverConstant;
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
  private String tablename;
  private String metaTablename;
  private String[] headers;

  /**
   * Constructor. Construct a HFile StoreFunc to write data out as HFiles. These HFiles will then have to be imported
   * with the hbase/bin/loadtable.rb tool.
   * @param tN The HBase table name the data will ultimately wind up in. It does not need to exist ahead of time.
   * @param cF The HBase column family name for the table the data will wind up it. It does not need to exist ahead of
   * time.
   * @param columnNames A comma separated list of column names descibing the fields in a tuple.
   */
  public DynamicStorage(String dataType, String releaseName)
  {
    this.tablename = dataType;
    if (releaseName == null || releaseName.trim().isEmpty()) {
      this.metaTablename = ArchiverConstant.META_TABLE_NAME;
    } else {
      this.metaTablename = SchemaUtil.getMetaTableName(releaseName);
    }
  }

  //
  // getOutputFormat()
  //
  // This method will be called by Pig to get the OutputFormat
  // used by the storer. The methods in the OutputFormat (and
  // underlying RecordWriter and OutputCommitter) will be
  // called by pig in the same manner (and in the same context)
  // as by Hadoop in a map-reduce java program. If the
  // OutputFormat is a hadoop packaged one, the implementation
  // should use the new API based one under
  // org.apache.hadoop.mapreduce. If it is a custom OutputFormat,
  // it should be implemented using the new API under
  // org.apache.hadoop.mapreduce. The checkOutputSpecs() method
  // of the OutputFormat will be called by pig to check the
  // output location up-front. This method will also be called as
  // part of the Hadoop call sequence when the job is launched. So
  // implementations should ensure that this method can be called
  // multiple times without inconsistent side effects.
  @Override
  public OutputFormat getOutputFormat() throws IOException {
    HFileOutputFormat2 outputFormat = new HFileOutputFormat2();
    return outputFormat;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    job.getConfiguration().set("mapred.textoutputformat.separator", "");
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    log.info("Resource Schema: " + s.toString());
    // first long type, second int type and third is a bag
    Preconditions.checkArgument(s.getFields().length == 3);

    UDFContext udfc = UDFContext.getUDFContext();
    Properties p =
        udfc.getUDFProperties(this.getClass());
    StringBuilder sb = new StringBuilder();

    for (ResourceFieldSchema field : headerFields(s.getFields()[2])) {
      sb.append(field.getName());
      sb.append(HEADER_SEPARATOR);
    }
    sb.setLength(sb.length() - HEADER_SEPARATOR.length());
    String headerline = sb.toString();
    p.setProperty("headerline", headerline);
    log.info("header information: {}", headerline);
  }

  /**
   * @param resourceFieldSchema
   * @return
   */
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

    Configuration conf = UDFContext.getUDFContext().getJobConf();
    ArchiveMetaManager metaManager = new ArchiveMetaManager(metaTablename, conf);
    metaManager.addHeader(tablename, headers);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void putNext(Tuple t) throws IOException {

    long rank = (long) t.get(0);
    int id = (int) t.get(1);
    DataBag bag = (DataBag) t.get(2);
    Tuple content = bag.iterator().next();

    byte[] rowKey = SchemaUtil.encodedArchiveRowKey(id, rank);
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