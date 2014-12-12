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
package org.icgc.dcc.etl.exporter.pig.udf;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.icgc.dcc.downloader.core.ArchiverConstant.DATA_CONTENT_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.HEADER_SEPARATOR;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;
import org.icgc.dcc.downloader.core.ArchiveMetaManager;
import org.icgc.dcc.downloader.core.ArchiverConstant;
import org.icgc.dcc.downloader.core.SchemaUtil;

import com.google.common.base.Preconditions;

public class TOHFILE extends EvalFunc<DataBag> {

  private static int BLOCKSIZE = 5 * 1048576;
  private static String COMPRESSION = Compression.Algorithm.SNAPPY.getName();
  private String[] headers;
  private final String destDir;
  private final String tablename;
  private Writer writer;
  private ArchiveMetaManager metaManager;
  private boolean hasDoneHeader = false;
  private int previousId;
  private final String metaTablename;
  private final boolean isPerGroupOutput;

  public TOHFILE(String dataType, String releaseName, String destDir,
      String isPerDonorOutputFile) throws IOException {
    super();
    this.destDir = destDir;
    this.tablename = dataType;
    this.isPerGroupOutput = Boolean.valueOf(isPerDonorOutputFile);
    if (releaseName == null || releaseName.trim().isEmpty()) {
      this.metaTablename = ArchiverConstant.META_TABLE_NAME;
    } else {
      this.metaTablename = SchemaUtil.getMetaTableName(releaseName);
    }
  }

  public TOHFILE(String dataType, String releaseName, String destDir)
      throws IOException {
    this(dataType, releaseName, destDir, "false");
  }

  private Writer createWriter(String donorId, UDFContext context)
      throws IOException {
    Configuration conf = UDFContext.getUDFContext().getJobConf();
    getLogger().info("Initializing a new HFile Writer...");
    FileSystem fs = FileSystem.get(conf);
    Path destPath = new Path(destDir, Bytes.toString(DATA_CONTENT_FAMILY));
    return HFile.getWriterFactory(conf, new CacheConfig(conf))
        .createWriter(fs, new Path(destPath, donorId), BLOCKSIZE,
            COMPRESSION, KeyValue.KEY_COMPARATOR);
  }

  private void closeWriter(Writer writer) {
    if (writer != null) {
      try {
        writer.close();
      } catch (IOException e) {
        getLogger().error("Fail to close the HFile writer", e);
      }
    }
  }

  /**
   * In the future, we can do a insert the donor id to integer id mapping to preserve the semantic of the search
   */
  @Override
  public DataBag exec(Tuple input) throws IOException {
    Configuration conf = UDFContext.getUDFContext().getJobConf();
    String donorId = input.get(0).toString();
    int id = Integer.valueOf(donorId);
    if (writer == null) {
      FileSystem fs = FileSystem.get(conf);
      Path destPath = new Path(destDir, Bytes.toString(DATA_CONTENT_FAMILY));
      if (!fs.exists(destPath)) fs.mkdirs(destPath);

      writer = createWriter(donorId, UDFContext.getUDFContext());
      metaManager = new ArchiveMetaManager(metaTablename, conf);
    } else if (id < previousId || isPerGroupOutput) {
      // close previous writer first
      closeWriter(writer);
      writer = createWriter(donorId, UDFContext.getUDFContext());
    }

    Properties props = UDFContext.getUDFContext().getUDFProperties(
        this.getClass());
    String headerline = props.getProperty("headerline");
    Preconditions.checkNotNull(headerline);
    headers = headerline.split(HEADER_SEPARATOR);

    DataBag bag = (DataBag) input.get(1);
    Iterator<Tuple> it = bag.iterator();

    long n = 1;
    DataBag output = BagFactory.getInstance().newDefaultBag();

    long totalBytes = 0;
    while (it.hasNext()) {
      Tuple t = it.next();
      byte[] rowKey = SchemaUtil.encodedArchiveRowKey(id, n);
      long now = System.currentTimeMillis();
      for (byte i = 0; i < headers.length; ++i) {
        Object cellValue = t.get(i);
        if (cellValue == null) continue;
        String value = (String) cellValue;
        if (value.trim().isEmpty()) continue;
        byte[] bytes = Bytes.toBytes(value);
        KeyValue kv = new KeyValue(rowKey, DATA_CONTENT_FAMILY,
            new byte[] { i }, now, bytes);
        totalBytes = totalBytes + bytes.length;
        writer.append(kv);
      }
      n++;
      reporter.progress();
    }

    if (n == 1) {
      RuntimeException ex = new RuntimeException(
          "No mutations for donor id: " + donorId);
      getLogger().warn("No mutations for donor id: " + donorId, ex);
      throw ex;
    }

    previousId = id;
    if (!hasDoneHeader) {
      metaManager.keepMetaInfo(tablename, id, --n, totalBytes, headers);
      hasDoneHeader = true;
    } else {
      metaManager.keepMetaInfo(tablename, id, --n, totalBytes);
    }

    Tuple result = TupleFactory.getInstance().newTuple(1);
    result.set(0, new DataByteArray(SchemaUtil.encodedArchiveRowKey(id, n)));
    output.add(result);
    return output;
  }

  @Override
  public Schema outputSchema(Schema input) {

    UDFContext context = UDFContext.getUDFContext();
    Properties udfProp = context.getUDFProperties(this.getClass());

    List<FieldSchema> outputFields = newArrayListWithCapacity(50);
    outputFields.add(new FieldSchema(null, DataType.BYTEARRAY));
    StringBuilder sb = new StringBuilder();
    try {
      for (FieldSchema field : input.getField(1).schema.getField(0).schema
          .getFields()) {
        sb.append(field.alias);
        sb.append(HEADER_SEPARATOR);
      }
      sb.setLength(sb.length() - HEADER_SEPARATOR.length());
      String headerline = sb.toString();
      udfProp.setProperty("headerline", headerline);
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }

    // DataType.BAG
    Schema recordSchema = new Schema(outputFields);
    try {
      return new Schema(new FieldSchema(null, recordSchema, DataType.BAG));
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finish() {
    closeWriter(writer);
    if (metaManager != null) {
      metaManager.close();
    }
  }

}