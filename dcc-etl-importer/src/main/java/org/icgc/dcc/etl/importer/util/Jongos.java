/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.importer.util;

import static com.fasterxml.jackson.databind.PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES;
import static lombok.AccessLevel.PRIVATE;

import java.io.IOException;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.jongo.Jongo;
import org.jongo.marshall.jackson.JacksonMapper;
import org.jongo.marshall.jackson.configuration.MapperModifier;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

@NoArgsConstructor(access = PRIVATE)
public final class Jongos {

  @SneakyThrows
  public static Jongo createJongo(@NonNull MongoClientURI mongoUri) {
    val mongo = new MongoClient(mongoUri);
    val db = mongo.getDB(mongoUri.getDatabase());

    return new Jongo(db, new JacksonMapper.Builder().addModifier(new MapperModifier() {

      @Override
      public void modify(ObjectMapper mapper) {
        mapper.setPropertyNamingStrategy(CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
        mapper.registerModule(
            new SimpleModule()
                .addSerializer(Enum.class, new EnumStdSerializer())
                .setDeserializerModifier(new EnumDeserializerModifier()));
      }

    }).build());
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static final class EnumDeserializerModifier extends BeanDeserializerModifier {

    @Override
    public JsonDeserializer<Enum> modifyEnumDeserializer(DeserializationConfig config, final JavaType type,
        BeanDescription beanDesc, final JsonDeserializer<?> deserializer) {
      return new JsonDeserializer<Enum>() {

        @Override
        public Enum deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
          Class<? extends Enum> rawClass = (Class<Enum<?>>) type.getRawClass();
          return Enum.valueOf(rawClass, jp.getValueAsString().toUpperCase());
        }

      };
    }
  }

  @SuppressWarnings("rawtypes")
  private static final class EnumStdSerializer extends StdSerializer<Enum> {

    private EnumStdSerializer() {
      super(Enum.class);
    }

    @Override
    public void serialize(Enum value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
      jgen.writeString(value.name().toLowerCase());
    }

  }

}
