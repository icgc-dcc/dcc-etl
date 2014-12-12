package org.icgc.dcc.etl.exporter.pig.udf;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

import com.twitter.elephantbird.pig.store.SequenceFileStorage;

public class MapFileStorage<K extends Writable, V extends Writable> extends SequenceFileStorage<K, V> {

  /**
   * @throws ParseException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public MapFileStorage() throws ParseException, IOException, ClassNotFoundException {
    super();
  }

  public MapFileStorage(String keyArgs, String valueArgs) throws ParseException, IOException, ClassNotFoundException {
    super(keyArgs, valueArgs);
  }

  public MapFileStorage(String keyArgs, String valueArgs, String otherArgs)
      throws ParseException, IOException,
      ClassNotFoundException {
    super(keyArgs, valueArgs, otherArgs);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public OutputFormat getOutputFormat() {
    return new MapFileOutputFormat();
  }

}
