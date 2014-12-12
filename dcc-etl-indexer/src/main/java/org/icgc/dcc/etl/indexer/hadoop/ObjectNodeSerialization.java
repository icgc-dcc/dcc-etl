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
package org.icgc.dcc.etl.indexer.hadoop;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import cascading.tuple.hadoop.SerializationToken;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;

/**
 * Base class for providing {@code ObjectNode} serialization to Smile format.
 * <p>
 * The reason for this class is that BSON is a terribly inefficent format in both space and time. It turns out
 * ElasticSearch is also using Smile for the same reasons. It also has the advantage of accepting Smile.
 */
@Slf4j
@SerializationToken(tokens = 225, classNames = "com.fasterxml.jackson.databind.node.ObjectNode")
public class ObjectNodeSerialization implements Serialization<ObjectNode> {

  static final JsonFactory FACTORY = new SmileFactory() //
      .disable(SmileGenerator.Feature.WRITE_HEADER) //
      .disable(SmileParser.Feature.REQUIRE_HEADER) //

      .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET) //
      .disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);

  static final ObjectMapper MAPPER = new ObjectMapper(FACTORY) //
      .disable(SerializationFeature.CLOSE_CLOSEABLE);

  static final ObjectWriter WRITER = MAPPER.writerWithType(ObjectNode.class);

  static final ObjectReader READER = MAPPER.reader(ObjectNode.class);

  @Override
  public boolean accept(Class<?> c) {
    return ObjectNode.class.isAssignableFrom(c);
  }

  @Override
  public Deserializer<ObjectNode> getDeserializer(Class<ObjectNode> c) {
    log.info("Creating {}...", ObjectNodeDeserializer.class.getSimpleName());
    return new ObjectNodeDeserializer(c);
  }

  @Override
  public Serializer<ObjectNode> getSerializer(Class<ObjectNode> c) {
    log.info("Creating {}...", ObjectNodeSerializer.class.getSimpleName());
    return new ObjectNodeSerializer(c);
  }

  class ObjectNodeSerializer implements Serializer<ObjectNode> {

    DataOutputStream out;

    ObjectNodeSerializer(Class<ObjectNode> clazz) {
    }

    @Override
    public void open(OutputStream out) throws IOException {
      if (out instanceof DataOutputStream) {
        this.out = (DataOutputStream) out;
      } else {
        this.out = new DataOutputStream(out);
      }
    }

    @Override
    public void close() throws IOException {
      out.close();
    }

    @Override
    public void serialize(ObjectNode ObjectNode) throws IOException {
      byte[] bytes = WRITER.writeValueAsBytes(ObjectNode);
      int length = bytes.length;
      out.writeInt(length);
      out.write(bytes);
      out.flush();
    }
  }

  class ObjectNodeDeserializer implements Deserializer<ObjectNode> {

    DataInputStream in;

    ObjectNodeDeserializer(Class<ObjectNode> clazz) {
    }

    @Override
    public void open(InputStream in) throws IOException {
      if (in instanceof DataInputStream) {
        this.in = (DataInputStream) in;
      } else {
        this.in = new DataInputStream(in);
      }
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public ObjectNode deserialize(ObjectNode next) throws IOException {
      int length = in.readInt();
      byte[] bytes = new byte[length];
      in.readFully(bytes);

      return READER.readValue(bytes);
    }

  }

}
