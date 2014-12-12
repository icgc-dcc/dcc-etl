package org.icgc.dcc.etl.exporter.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import lombok.SneakyThrows;

import org.apache.commons.compress.utils.Charsets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.icgc.dcc.etl.exporter.hadoop.DataFileInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

public class DataFileInputFormatTest {

  private static final String BASE_PATH = "/tmp/" + DataFileInputFormatTest.class.getSimpleName();

  private JobContext job;

  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    job = new Job();
    fs = FileSystem.get(job.getConfiguration());
    FileInputFormat.addInputPath(new Job(job.getConfiguration()), new Path(BASE_PATH));
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(new Path(BASE_PATH), true);
  }

  @Test
  @SneakyThrows
  public void sanity() {
    Random rand = new Random();
    byte[] testbytes = new byte[1024];
    rand.nextBytes(testbytes);
    insertTestData(testbytes, "input");
    DataFileInputFormat format = new DataFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);

    assertEquals(1, splits.size());
    assertEquals(testbytes.length, splits.get(0).getLength());
  }

  @Test
  @SneakyThrows
  public void noSplits() {
    Random rand = new Random();
    byte[] testbytes = new byte[134217728];
    rand.nextBytes(testbytes);
    insertTestData(testbytes, "input");
    DataFileInputFormat format = new DataFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    assertEquals(1, splits.size());
    assertEquals(testbytes.length, splits.get(0).getLength());
  }

  @Test
  @SneakyThrows
  public void multipleFiles() {
    Random rand = new Random();
    int NUM_FILES = 5;
    byte[] testbytes = new byte[1024];

    for (int i = 0; i < NUM_FILES; ++i) {
      rand.nextBytes(testbytes);
      insertTestData(testbytes, "input_" + i);
    }

    DataFileInputFormat format = new DataFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);

    assertEquals(1, splits.size());
    assertTrue(splits.get(0) instanceof CombineFileSplit);
    CombineFileSplit cfs = (CombineFileSplit) splits.get(0);

    assertEquals(NUM_FILES, cfs.getPaths().length);
    Arrays.sort(cfs.getPaths(), new Comparator<Path>() {

      @Override
      public int compare(Path thiz, Path that) {
        return ComparisonChain.start().compare(thiz.getName(), that.getName()).result();
      }
    });
    for (int i = 0; i < NUM_FILES; ++i) {
      assertEquals(testbytes.length, cfs.getLength(i));
      assertTrue("i: " + i + ", Path: " + cfs.getPath(i), cfs.getPath(i).getName().equals("input_" + i));
    }
  }

  @SneakyThrows
  @Test
  public void reader() {
    Random rand = new Random();
    int NUM_FILES = 5;

    List<byte[]> testdata = Lists.newArrayListWithCapacity(NUM_FILES);
    for (int i = 0; i < NUM_FILES; ++i) {
      byte[] testbytes = new byte[1024];
      rand.nextBytes(testbytes);
      testdata.add(testbytes);
      insertTestData(testbytes, "input_" + i);
    }

    DataFileInputFormat format = new DataFileInputFormat();

    List<InputSplit> splits = format.getSplits(job);
    assertEquals(1, splits.size());

    TaskAttemptContext attemptContext = mock(TaskAttemptContext.class);
    when(attemptContext.getConfiguration()).thenReturn(job.getConfiguration());

    RecordReader<Text, StreamingOutputBytesWritable> reader = format.createRecordReader(splits.get(0), attemptContext);

    List<byte[]> actualData = Lists.newArrayList();
    int numKV = 0;
    while (reader.nextKeyValue()) {
      actualData.add(reader.getCurrentValue().copyBytes());
      numKV++;
    }
    assertEquals(NUM_FILES, numKV);

    for (int i = 0; i < NUM_FILES; ++i) {
      boolean found = false;
      for (int j = 0; j < actualData.size(); ++j) {
        if (Arrays.equals(actualData.get(j), testdata.get(i))) found = true;
      }
      assertTrue(found);
    }
  }

  @SneakyThrows
  @Test
  public void convertBackToString() {
    String text = "This is an apple.";
    byte[] testbytes = text.getBytes(Charsets.UTF_8);
    insertTestData(testbytes, "input");

    DataFileInputFormat format = new DataFileInputFormat();
    List<InputSplit> splits = format.getSplits(job);

    TaskAttemptContext attemptContext = mock(TaskAttemptContext.class);
    when(attemptContext.getConfiguration()).thenReturn(job.getConfiguration());

    RecordReader<Text, StreamingOutputBytesWritable> reader = format.createRecordReader(splits.get(0), attemptContext);
    reader.nextKeyValue();
    StreamingOutputBytesWritable value = reader.getCurrentValue();
    String actualText = new String(value.copyBytes(), Charsets.UTF_8);

    assertTrue(actualText + " != " + text, actualText.equals(text));

  }

  private void insertTestData(byte[] input, String filename) throws IOException {
    OutputStream out = fs.create(new Path(BASE_PATH, filename));
    out.write(input);
    out.close();
  }
}
