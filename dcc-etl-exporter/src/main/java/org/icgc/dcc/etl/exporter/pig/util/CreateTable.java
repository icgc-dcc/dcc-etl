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
package org.icgc.dcc.etl.exporter.pig.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.icgc.dcc.downloader.core.ArchiveCompositeKey;
import org.icgc.dcc.downloader.core.SchemaUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

/**
 * It is used for bucket.pig for load-balancing purposes
 *
 */
public class CreateTable extends EvalFunc<Long> {

  private final int numRegions;
  private final String tablename;
  private final static String HBASE_CONFIG_SET = "hbase.config.set";
  private Configuration localConf;

  public CreateTable(String tablename, String numRegions) throws IOException {
    super();
    this.tablename = tablename;
    this.numRegions = Integer.valueOf(numRegions);
    Preconditions.checkArgument(this.numRegions > 0);
    initializeLocalJobConfig();
  }

  private void initializeLocalJobConfig() {
    localConf = new Configuration();
    Properties udfProps = UDFContext.getUDFContext().getUDFProperties(
        this.getClass());
    if (udfProps.containsKey(HBASE_CONFIG_SET)) {
      for (Entry<Object, Object> entry : udfProps.entrySet()) {
        if ((entry.getKey() instanceof String)
            && (entry.getValue() instanceof String)) {
          localConf.set((String) entry.getKey(),
              (String) entry.getValue());
        }
      }
    } else {
      Configuration hbaseConf = HBaseConfiguration.create();
      for (Entry<String, String> entry : hbaseConf) {
        // JobConf may have some conf overriding ones in hbase-site.xml
        // So only copy hbase config not in job config to UDFContext
        // Also avoids copying core-default.xml and core-site.xml
        // props in hbaseConf to UDFContext which would be redundant.
        udfProps.setProperty(entry.getKey(), entry.getValue());
        localConf.set(entry.getKey(), entry.getValue());
      }
      udfProps.setProperty(HBASE_CONFIG_SET, "true");
    }
  }

  /**
   * In the future, we can do a insert the donor id to integer id mapping to preserve the semantic of the search
   */
  @Override
  public Long exec(Tuple input) throws IOException {
    DataBag bag = (DataBag) input.get(0);
    Iterator<Tuple> it = bag.iterator();
    long total = 0;
    RangeMap<Long, ArchiveCompositeKey> rangeMap = TreeRangeMap.create();
    byte[] previousKey = new byte[0];
    while (it.hasNext()) {
      Tuple t = it.next();
      DataByteArray bytes = (DataByteArray) t.get(0);
      byte[] currentKey = bytes.get();

      if (Bytes.compareTo(previousKey, currentKey) > 0) {
        throw new RuntimeException("The key has to be sorted.");
      }

      ArchiveCompositeKey key = SchemaUtil.decodeArchiveRowKey(currentKey);
      long openRange = total;
      total = total + key.getLineNum();
      rangeMap.put(Range.openClosed(openRange, total), key);
      previousKey = currentKey;
    }

    // No data, just create an empty table
    if (total == 0) {
      SchemaUtil.createDataTable(tablename, localConf);
      return 0L;
    } else {

      long keysPerRS = total / numRegions + 1;
      Builder<byte[]> builder = ImmutableList.builder();

      for (int i = 1; i < numRegions; ++i) {
        long splitPoint = i * keysPerRS;
        Entry<Range<Long>, ArchiveCompositeKey> rangeEntry = rangeMap
            .getEntry(splitPoint);
        if (rangeEntry == null) break;
        Range<Long> range = rangeEntry.getKey();
        byte[] split = SchemaUtil.encodedArchiveRowKey(rangeEntry
            .getValue().getDonorId(), splitPoint - range.lowerEndpoint());

        getLogger().info("Split #: " + i + ", CompositeKey: " + rangeEntry.getValue() + ", Range: " + range.toString());

        builder.add(split);
      }
      getLogger().info("Number of Keys per region server: " + keysPerRS);

      SchemaUtil.createDataTable(tablename, builder.build(), localConf);
      return keysPerRS;
    }

  }
}