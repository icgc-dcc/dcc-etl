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

import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.icgc.dcc.etl.exporter.hadoop.StreamingOutputBytesWritable;
import org.icgc.dcc.etl.exporter.util.ExporterValidation;

/**
 * 
 */
@Slf4j
public class DynamicDownloadExporter {

  private final static String VALIDATION_DIR_DEFAULT = "/tmp/validation";

  public static void main(String[] args) throws Exception {
    String inputDir = args[0];
    String outputDir = args[1];
    String bufferSize = args[2];
    String validationDir = args[3];

    run(inputDir, outputDir, validationDir, Integer.parseInt(bufferSize));
  }

  public static void run(String inputDir, String outputDir) throws Exception {
    run(inputDir, outputDir, VALIDATION_DIR_DEFAULT, 5242880);
  }

  public static void run(String inputDir, String outputDir, int bufferSize) throws Exception {
    run(inputDir, outputDir, VALIDATION_DIR_DEFAULT, bufferSize);
  }

  public static void run(String inputDir, String outputDir, String validationDir, int bufferSize) throws Exception {

    Path inputPath = new Path(inputDir);
    Path outputPath = new Path(outputDir);

    Configuration conf = new Configuration();
    conf.setInt("io.map.index.interval", 1);
    conf.set("zlib.compress.level", "BEST_SPEED");

    FileSystem fs = FileSystem.get(URI.create(inputDir), conf);
    if (fs.exists(outputPath)) fs.delete(outputPath, true);
    fs.mkdirs(outputPath);

    FileStatus[] seqs = fs.listStatus(inputPath, new PathFilter() {

      @Override
      public boolean accept(Path path) {
        if (path.getName().startsWith("part")) return true;
        return false;
      }
    });

    Arrays.sort(seqs, new Comparator<FileStatus>() {

      @Override
      public int compare(FileStatus thisStatus, FileStatus thatStatus) {
        return thisStatus.getPath().getName().compareTo(thatStatus.getPath().getName());
      }
    });

    if (seqs.length > 0) {
      log.info("Number of sequence files to processed: {}", seqs.length);
      SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(seqs[0].getPath()));
      final Text readerKey = new Text();
      final StreamingOutputBytesWritable readerValue = new StreamingOutputBytesWritable();
      final CompressionCodec codec = reader.getCompressionCodec();
      final CompressionType type = reader.getCompressionType();
      reader.close();
      final Writer writer = new Writer(conf, outputPath, Writer.keyClass(readerKey.getClass()),
          Writer.valueClass(readerValue.getClass()), Writer.compression(type, codec));

      try {
        int remaining = seqs.length;
        for (val seq : seqs) {
          log.info("Start Processing: " + seq.getPath() + "; Left: " + --remaining);
          reader =
              new SequenceFile.Reader(conf, Reader.file(seq.getPath()), Reader.bufferSize(bufferSize));
          try {
            while (reader.next(readerKey, readerValue)) {
              writer.append(readerKey, readerValue);
            }
          } finally {
            reader.close();
          }
        }
        // Create the map file index file
      } finally {
        writer.close();
        log.info("Export Completed.");
        log.info("Data Export Validation Starts ... ");
        ExporterValidation.main(new String[] { outputDir + "/data", validationDir });
        log.info("Data Export Validation Completed");
        log.info("Check the corrupted index...");
        SequenceFile.Reader validationReader =
            new SequenceFile.Reader(conf, Reader.file(new Path(validationDir + "/part-r-00000")));
        try {
          long errorCount = 0;
          while (validationReader.next(readerKey)) {
            errorCount++;
          }
          if (errorCount != 0) {
            throw new RuntimeException("Index is corrupted. Number of corrupted index is: " + errorCount
                + ". Please check the validation file at: " + validationDir);
          }

        } finally {
          validationReader.close();
        }
      }
    }
  }
}