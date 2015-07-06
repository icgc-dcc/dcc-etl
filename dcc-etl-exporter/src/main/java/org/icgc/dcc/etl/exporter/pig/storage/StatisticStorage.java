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

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextOutputFormat;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
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
public class StatisticStorage extends PigStorage {

  private String dataType;
  private String metaTablename;
  private final byte DELIMITER = '\t';

  public StatisticStorage(String dataType, String releaseName)
  {
    this.dataType = dataType;
    if (releaseName == null || releaseName.trim().isEmpty()) {
      this.metaTablename = ArchiverConstant.META_TABLE_NAME;
    } else {
      this.metaTablename = SchemaUtil.getMetaTableName(releaseName);
    }
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    log.info("Resource Schema: " + s.toString());
    Preconditions.checkArgument(s.getFields().length == 3);
    Preconditions.checkArgument(s.getFields()[0].getName().equals("donor_id"));
    Preconditions.checkArgument(s.getFields()[1].getName().equals("total_line"));
    Preconditions.checkArgument(s.getFields()[2].getName().equals("total_size"));

  }

  @Override
  public OutputFormat<?, ?> getOutputFormat() {
    return new StatisticOutputFormat(dataType, metaTablename, DELIMITER);
  }

  /**
   * Internal OutputFormat class
   */
  public static class StatisticOutputFormat extends PigTextOutputFormat {

    final private String dataType;

    /**
     * Delimiter to use between fields
     */
    final private byte fieldDelimiter;

    final private String metaTablename;

    public StatisticOutputFormat(String dataType, String metaTablename, byte delimiter) {
      super(delimiter);
      this.fieldDelimiter = delimiter;
      this.dataType = dataType;
      this.metaTablename = metaTablename;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public RecordWriter<WritableComparable, Tuple> getRecordWriter(
        TaskAttemptContext context) throws IOException,
        InterruptedException {

      Configuration conf = context.getConfiguration();

      FileSystem fs = FileSystem.get(conf);
      Path file = this.getDefaultWorkFile(context, "");
      FSDataOutputStream fileOut = fs.create(file, false);

      ArchiveMetaManager metaManager = new ArchiveMetaManager(metaTablename, conf);
      return new StatisticStorageRecordWriter(fileOut, this.fieldDelimiter, this.dataType, metaManager);
    }

    /**
     * Internal class to do the actual record writing and index generation
     *
     */
    public static class StatisticStorageRecordWriter extends PigLineRecordWriter {

      final private String dataType;

      /**
       * Index builder
       */
      final private ArchiveMetaManager metaManager;

      public StatisticStorageRecordWriter(FSDataOutputStream fileOut, byte fieldDel, String dataType,
          ArchiveMetaManager metaManager)
          throws IOException {
        super(fileOut, fieldDel);
        this.metaManager = metaManager;
        this.dataType = dataType;

      }

      @Override
      public void write(@SuppressWarnings("rawtypes") WritableComparable key, Tuple value) throws IOException {
        super.write(key, value);
        metaManager.keepMetaInfo(dataType, (int) value.get(0), (long) value.get(1), (long) value.get(2));
      }

      @Override
      public void close(TaskAttemptContext context)
          throws IOException {
        this.metaManager.close();
        super.close(context);
      }

    }

  }
}