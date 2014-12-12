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
package org.icgc.dcc.etl.exporter.util;

import java.io.IOException;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.icgc.dcc.downloader.core.Utils;
import org.icgc.dcc.downloader.core.Utils.CompositeKey;
import org.icgc.dcc.etl.exporter.hadoop.StreamingOutputBytesWritable;

/**
 * 
 */
public class ExporterValidation extends Configured implements Tool {

  private static class ValidationMapper extends Mapper<Text, StreamingOutputBytesWritable, Text, NullWritable> {

    private final static NullWritable EMPTY = NullWritable.get();

    @Override
    public void map(Text key, StreamingOutputBytesWritable content, Context context) throws IOException,
        InterruptedException {

      byte[] bytes = content.copyBytes();
      CompositeKey parts = Utils.getCompositeKey(key);
      if (parts.size != bytes.length) {
        context.write(key, EMPTY);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {
    Job job = createJob(getConf(), new Path(args[0]), new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  @SneakyThrows
  public Job createJob(Configuration conf, Path inputPath, Path outputPath) {
    Job job = new Job(conf);
    job.setJarByClass(ExporterValidation.class);

    // specify the input & output formats
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapperClass(ValidationMapper.class);

    // specify the configuration for mapper and reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setNumReduceTasks(1);
    return job;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExporterValidation(), args);
  }
}
