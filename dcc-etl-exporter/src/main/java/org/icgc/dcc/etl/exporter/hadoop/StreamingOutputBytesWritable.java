package org.icgc.dcc.etl.exporter.hadoop;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.activation.UnsupportedDataTypeException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BytesWritable;

public class StreamingOutputBytesWritable extends BytesWritable {

  private final OutputStream out;

  private boolean skip;

  public StreamingOutputBytesWritable() {
    super();
    out = null;
    skip = false;
  }

  public StreamingOutputBytesWritable(OutputStream out) {
    this.out = out;
  }

  public StreamingOutputBytesWritable(byte[] testdata) {
    super(testdata);
    out = null;
  }

  public StreamingOutputBytesWritable(byte[] bytes, int length) {
    super(bytes, length);
    out = null;
  }

  void setSkip(boolean skip) {
    this.skip = skip;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (out == null) super.readFields(in);
    else {
      if (in instanceof InputStream) {
        // important! Discard the first 4 bytes.
        int numBytes = in.readInt();
        if (skip) {
          // Skip all bytes
          IOUtils.skipFully((InputStream) in, numBytes);
        } else {
          IOUtils.copy((InputStream) in, out);
        }
      } else {
        throw new UnsupportedDataTypeException();
      }
    }
  }
}
