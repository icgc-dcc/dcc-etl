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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.builtin.PigStorage;
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
public class StatisticStorage extends PigStorage {

  private String dataType;
  private String metaTablename;
  private ArchiveMetaManager metaManager;

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
  public void prepareToWrite(RecordWriter writer) {
    super.prepareToWrite(writer);
    Configuration conf = UDFContext.getUDFContext().getJobConf();
    try {
      metaManager = new ArchiveMetaManager(metaTablename, conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  };

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    log.info("Resource Schema: " + s.toString());
    Preconditions.checkArgument(s.getFields().length == 3);
    Preconditions.checkArgument(s.getFields()[0].getName().equals("donor_id"));
    Preconditions.checkArgument(s.getFields()[1].getName().equals("total_line"));
    Preconditions.checkArgument(s.getFields()[2].getName().equals("total_size"));

  }

  @Override
  public void putNext(Tuple t) throws IOException {
    metaManager.keepMetaInfo(dataType, (int) t.get(0), (long) t.get(1), (long) t.get(2));
    super.putNext(t);

  }
}