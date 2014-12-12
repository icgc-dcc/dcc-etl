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
package org.icgc.dcc.etl.loader.cascading;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import cascading.CascadingException;
import cascading.tuple.Comparison;
import cascading.tuple.Fields;
import cascading.tuple.StreamComparator;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.hadoop.SerializationToken;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.io.BufferedInputStream;
import cascading.tuple.hadoop.io.TupleDeserializer;
import cascading.tuple.hadoop.io.TupleSerializer;

import com.google.common.base.Throwables;

/**
 * Serialization implementation that is required for Hadoop {@link SequenceFile} serialization.
 */
@SerializationToken(tokens = { 223 }, classNames = { "cascading.tuple.TupleEntry" })
public class TupleEntrySerialization extends Configured implements Comparison<TupleEntry>, Serialization<TupleEntry> {

  /**
   * Delegate for recursively contained {@link Tuple} values.
   */
  private TupleSerialization tupleSerialization;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);

    this.tupleSerialization = new TupleSerialization(conf);
  }

  @Override
  public boolean accept(Class<?> c) {
    return TupleEntry.class.isAssignableFrom(c);
  }

  @Override
  public Serializer<TupleEntry> getSerializer(Class<TupleEntry> c) {
    final TupleSerializer tupleSerializer = (TupleSerializer) tupleSerialization.getSerializer(Tuple.class);

    return new TupleEntrySerializer(tupleSerializer);
  }

  @Override
  public Deserializer<TupleEntry> getDeserializer(Class<TupleEntry> c) {
    final TupleDeserializer tupleDeserializer = (TupleDeserializer) tupleSerialization.getDeserializer(Tuple.class);

    return new TupleEntryDeserializer(tupleDeserializer);
  }

  @Override
  public Comparator<TupleEntry> getComparator(Class<TupleEntry> arg0) {
    return new TupleEntryComparator();
  }

  /**
   * {@code TupleEntry} serializer implementation.
   */
  public static class TupleEntrySerializer implements Serializer<TupleEntry> {

    private DataOutputStream out;

    private final TupleSerializer delegate;

    public TupleEntrySerializer(TupleSerializer tupleSerializer) {
      this.delegate = tupleSerializer;
    }

    @Override
    public void open(OutputStream out) throws IOException {
      delegate.open(out);

      if (out instanceof DataOutputStream) {
        this.out = (DataOutputStream) out;
      } else {
        this.out = new DataOutputStream(out);
      }
    }

    @Override
    public void serialize(TupleEntry t) throws IOException {
      ObjectOutputStream output = new ObjectOutputStream(out);
      output.writeObject(t.getFields());

      delegate.serialize(t.getTuple());
    }

    @Override
    public void close() throws IOException {
      try {
        delegate.close();
      } finally {
        out.close();
      }
    }

  }

  /**
   * {@code TupleEntry} deserializer implementation.
   */
  public static class TupleEntryDeserializer implements Deserializer<TupleEntry> {

    private DataInputStream in;

    private final TupleDeserializer delegate;

    public TupleEntryDeserializer(TupleDeserializer tupleDeserializer) {
      this.delegate = tupleDeserializer;
    }

    @Override
    public void open(InputStream in) throws IOException {
      delegate.open(in);

      if (in instanceof DataInputStream) {
        this.in = (DataInputStream) in;
      } else {
        this.in = new DataInputStream(in);
      }
    }

    @Override
    public TupleEntry deserialize(TupleEntry t) throws IOException {
      ObjectInputStream input = new ObjectInputStream(in);
      TupleEntry tupleEntry = null;
      try {
        Fields fields = (Fields) input.readObject();
        Tuple tuple = delegate.deserialize(null);

        tupleEntry = new TupleEntry(fields, tuple);
      } catch (ClassNotFoundException e) {
        Throwables.propagate(e);
      }

      return tupleEntry;
    }

    @Override
    public void close() throws IOException {
      try {
        delegate.close();
      } finally {
        in.close();
      }
    }

  }

  public static class TupleEntryComparator implements StreamComparator<BufferedInputStream>, Comparator<TupleEntry>,
      Serializable {

    @Override
    public int compare(TupleEntry lhs, TupleEntry rhs) {
      if (lhs == null) {
        return -1;
      }

      if (rhs == null) {
        return 1;
      }

      return 0;
    }

    @Override
    public int compare(BufferedInputStream lhsStream, BufferedInputStream rhsStream) {
      try {
        if (lhsStream == null && rhsStream == null) {
          return 0;
        }

        if (lhsStream == null) {
          return -1;
        }

        if (rhsStream == null) {
          return 1;
        }

        String lhsString = WritableUtils.readString(new DataInputStream(lhsStream));
        String rhsString = WritableUtils.readString(new DataInputStream(rhsStream));

        return lhsString.compareTo(rhsString);
      } catch (IOException exception) {
        throw new CascadingException(exception);
      }
    }

  }

}
