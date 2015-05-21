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
package org.icgc.dcc.etl.exporter.core;

import static org.icgc.dcc.downloader.core.ArchiverConstant.DATA_CONTENT_FAMILY;

import java.util.Arrays;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.util.Bytes;
import org.icgc.dcc.downloader.core.SchemaUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

/**
 * It is used for bucket.pig for load-balancing purposes
 * 
 */
@Slf4j
public class LoadBalancer {

  @SneakyThrows
  public static boolean canLoadBalance(String hfileDir) {
    Path inputPath = new Path(hfileDir, Bytes.toString(DATA_CONTENT_FAMILY));
    Configuration conf = HBaseConfiguration.create();
    try (FileSystem fs = FileSystem.get(conf)) {
      return fs.exists(inputPath);
    }
  }

  @SneakyThrows
  public static void run(String tablename, String hfileDir) {
    Path inputPath = new Path(hfileDir, Bytes.toString(DATA_CONTENT_FAMILY));
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] partStatus = fs.listStatus(inputPath, new PathFilter() {

      @Override
      public boolean accept(Path path) {
        // remove hadoop tmp directories from the path status
        return !path.getName().startsWith("_");
      }
    });

    if (partStatus.length < 2) {
      // no need to load balance
      log.info("1 or less part files are found. No balancing.");
      return;
    }

    partStatus = Arrays.copyOfRange(partStatus, 1, partStatus.length);

    HTableDescriptor schema = SchemaUtil.getDataTableSchema(tablename);

    Builder<byte[]> splitKeys = ImmutableList.builder();
    for (FileStatus part : partStatus) {
      if (part.isFile()) {
        try (Reader reader = HFile.createReader(fs, part.getPath(), new CacheConfig(conf), conf)) {
          if (reader.getCompressionAlgorithm() != schema.getFamily(DATA_CONTENT_FAMILY).getCompression()) {
            log.error("wrong compression: {}", reader.getCompressionAlgorithm());
            throw new RuntimeException("Wrong file format");
          }
          splitKeys.add(reader.getFirstRowKey());
          log.info("Split Key: {}", Bytes.toString(reader.getFirstRowKey()));
        }
      }
    }
    SchemaUtil.createDataTable(tablename, splitKeys.build(), conf);
  }
}