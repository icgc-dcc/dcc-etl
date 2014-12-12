package org.icgc.dcc.etl.exporter;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.icgc.dcc.etl.exporter.hadoop.DataFileInputFormat;
import org.icgc.dcc.etl.exporter.hadoop.StreamingOutputBytesWritable;

public class DataIndexer extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Job job = createJob(getConf(), new Path(args[0]), new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  @SneakyThrows
  public Job createJob(Configuration conf, Path inputPath, Path outputPath) {
    Job job = new Job(conf);
    job.setJarByClass(DataIndexer.class);

    // specify the input & output formats
    job.setInputFormatClass(DataFileInputFormat.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    // specify the configuration for mapper and reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(StreamingOutputBytesWritable.class);
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    return job;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new DataIndexer(), args);
    System.exit(exitCode);
  }
}