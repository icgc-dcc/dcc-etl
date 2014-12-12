package org.icgc.dcc.etl.exporter.hadoop;

import static org.icgc.dcc.downloader.core.Utils.KEY_SEPARATOR;

import java.io.IOException;
import java.util.regex.Pattern;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class DataFileRecordReader extends RecordReader<Text, StreamingOutputBytesWritable> {

  private static final String UNKNOWN = "UNKNOWN";

  private FileSplit fileSplit;

  private Configuration conf;

  private final StreamingOutputBytesWritable value = new StreamingOutputBytesWritable();

  private final Text key = new Text();

  private boolean processed = false;

  @SneakyThrows
  public DataFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer idx) {
    fileSplit = new FileSplit(split.getPath(idx), split.getOffset(idx), split.getLength(idx), split.getLocations());
    conf = context.getConfiguration();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    // no need to initialize
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!processed) {
      byte[] contents = new byte[(int) fileSplit.getLength()];
      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(conf);
      FSDataInputStream in = null;
      try {
        in = fs.open(file);
        IOUtils.readFully(in, contents, 0, contents.length);
        value.set(contents, 0, contents.length);
        String[] parts = file.getName().split(Pattern.quote(KEY_SEPARATOR));
        switch (parts.length) {
        case 1:
          key.set(parts[0] + KEY_SEPARATOR + UNKNOWN + KEY_SEPARATOR + UNKNOWN + KEY_SEPARATOR + UNKNOWN
              + KEY_SEPARATOR + 2 + KEY_SEPARATOR + contents.length);
          break;
        case 2:
          key.set(parts[0] + KEY_SEPARATOR + parts[1] + KEY_SEPARATOR + UNKNOWN + KEY_SEPARATOR + UNKNOWN
              + KEY_SEPARATOR + 2 + KEY_SEPARATOR + contents.length);
          break;
        case 3:
          key.set(parts[0] + KEY_SEPARATOR + parts[1] + KEY_SEPARATOR + UNKNOWN + KEY_SEPARATOR
              + parts[2].toLowerCase() + KEY_SEPARATOR + 2 + KEY_SEPARATOR + contents.length);
          break;
        case 4:
          key.set(parts[0] + KEY_SEPARATOR + parts[1] + KEY_SEPARATOR + parts[3].toLowerCase() + KEY_SEPARATOR
              + parts[2].toLowerCase() + KEY_SEPARATOR + 2 + KEY_SEPARATOR + contents.length);
          break;
        default:
          key.set(UNKNOWN + KEY_SEPARATOR + UNKNOWN + KEY_SEPARATOR + UNKNOWN + KEY_SEPARATOR + UNKNOWN + KEY_SEPARATOR
              + 2 + KEY_SEPARATOR + contents.length);
        }

      } finally {
        IOUtils.closeStream(in);
      }
      processed = true;
      return true;
    }
    return false;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public StreamingOutputBytesWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException {
    return processed ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}