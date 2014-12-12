package org.icgc.dcc.etl.exporter;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.mock.MockInputSplit;
import org.icgc.dcc.etl.exporter.hadoop.StreamingOutputBytesWritable;
import org.junit.Before;
import org.junit.Test;

public class DataIndexerTest {

  MapDriver<Text, StreamingOutputBytesWritable, Text, StreamingOutputBytesWritable> mapDriver;
  ReduceDriver<Text, StreamingOutputBytesWritable, Text, StreamingOutputBytesWritable> reduceDriver;
  MapReduceDriver<Text, StreamingOutputBytesWritable, Text, StreamingOutputBytesWritable, Text, StreamingOutputBytesWritable> mapReduceDriver;

  @Before
  public void setUp() {
    Mapper<Text, StreamingOutputBytesWritable, Text, StreamingOutputBytesWritable> mapper =
        new Mapper<Text, StreamingOutputBytesWritable, Text, StreamingOutputBytesWritable>();
    mapDriver = MapDriver.newMapDriver(mapper);
    Reducer<Text, StreamingOutputBytesWritable, Text, StreamingOutputBytesWritable> reducer =
        new Reducer<Text, StreamingOutputBytesWritable, Text, StreamingOutputBytesWritable>();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() {
    Random rand = new Random();
    byte[] testdata = new byte[1024];
    rand.nextBytes(testdata);

    mapDriver.withInput(new Text("somefile"), new StreamingOutputBytesWritable(testdata));
    mapDriver.withOutput(new Text(MockInputSplit.getMockPath().getName()), new StreamingOutputBytesWritable(testdata));
    mapDriver.runTest();
  }

  @Test
  public void testReducer() {
    Random rand = new Random();
    byte[] testdata = new byte[1024];
    rand.nextBytes(testdata);

    Text testKey = new Text(MockInputSplit.getMockPath().getName());
    List<StreamingOutputBytesWritable> values = newArrayList();
    values.add(new StreamingOutputBytesWritable(testdata));
    reduceDriver.withInput(testKey, values);
    reduceDriver.withOutput(testKey, new StreamingOutputBytesWritable(testdata));
    reduceDriver.runTest();
  }
}
