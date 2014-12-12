package org.icgc.dcc.etl.exporter.pig.udf;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.icgc.dcc.etl.exporter.hadoop.StreamingOutputBytesWritable;

import com.twitter.elephantbird.pig.util.AbstractWritableConverter;

public class StreamingOutputBytesWritableConverter extends AbstractWritableConverter<StreamingOutputBytesWritable> {

  private final DataInputBuffer in = new DataInputBuffer();

  public StreamingOutputBytesWritableConverter() {
    super(new StreamingOutputBytesWritable());
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceFieldSchema schema = new ResourceFieldSchema();
    schema.setType(DataType.BYTEARRAY);
    return schema;
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    byte[] bytes = dataByteArray.get();
    // test leading 4 bytes encode run length of rest of data
    in.reset(bytes, bytes.length);
    int length = in.readInt();
    if (length != bytes.length - 4) {
      throw new IOException(String.format(
          "Int value '%d' of leading four bytes does not match run length of data '%d'",
          length, bytes.length - 4));
    }
    return new DataByteArray(bytes, 4, bytes.length);
  }

  @Override
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException {
    switch (schema.getType()) {
    case DataType.BYTEARRAY:
      return;
    }
    throw new IOException("Pig type '" + DataType.findTypeName(schema.getType()) + "' unsupported");
  }

  @Override
  protected StreamingOutputBytesWritable toWritable(DataByteArray value) throws IOException {
    return new StreamingOutputBytesWritable(value.get());
  }
}
