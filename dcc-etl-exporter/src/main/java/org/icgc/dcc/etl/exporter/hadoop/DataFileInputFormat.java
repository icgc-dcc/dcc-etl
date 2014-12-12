package org.icgc.dcc.etl.exporter.hadoop;

import java.io.IOException;

import lombok.SneakyThrows;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

public class DataFileInputFormat extends
    CombineFileInputFormat<Text, StreamingOutputBytesWritable> {

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }

  @SneakyThrows
  @Override
  public RecordReader<Text, StreamingOutputBytesWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<Text, StreamingOutputBytesWritable>((CombineFileSplit) split, context,
        DataFileRecordReader.class);
  }

}