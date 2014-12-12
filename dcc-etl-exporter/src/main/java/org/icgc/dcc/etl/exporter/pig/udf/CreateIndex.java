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
package org.icgc.dcc.etl.exporter.pig.udf;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.icgc.dcc.downloader.core.Utils.DELIM_SEPARATOR;
import static org.icgc.dcc.downloader.core.Utils.KEY_SEPARATOR;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStatusReporter;
import org.icgc.dcc.downloader.core.Utils.STREAM_STATE;

public class CreateIndex extends EvalFunc<DataBag> {

  private String headerline;
  private final static int DEFAULT_BLOCK_LENGTH = 20480;
  private final static int REPORT_EXCEED_SPLITS = 5;
  private final int blockLength;

  private boolean isBegin;

  public CreateIndex() {
    super();
    blockLength = DEFAULT_BLOCK_LENGTH;
  }

  public CreateIndex(String numLines) {
    blockLength = Integer.parseInt(numLines);
  }

  @Override
  public DataBag exec(Tuple input) throws IOException {
    isBegin = true;
    StringBuilder sb = new StringBuilder();
    if (headerline == null) {
      Properties props = UDFContext.getUDFContext().getUDFProperties(this.getClass());
      String headers = props.getProperty("headerline");
      for (String header : headers.split(",")) {
        sb.append(header);
        sb.append(DELIM_SEPARATOR);
      }
      sb.setLength(sb.length() - DELIM_SEPARATOR.length());
      sb.append(System.getProperty("line.separator"));
      this.headerline = sb.toString();
    } else {
      sb.append(headerline);
    }

    DataBag bag = (DataBag) input.get(1);
    Iterator<Tuple> it = bag.iterator();

    Tuple key = (Tuple) input.get(0);
    long n = 1;
    int numSplits = 0;
    DataBag output = BagFactory.getInstance().newDefaultBag();
    while (it.hasNext()) {
      Tuple t = it.next();
      sb.append(t.toDelimitedString(DELIM_SEPARATOR));
      sb.append(System.getProperty("line.separator"));
      if ((n % blockLength) == 0) {
        output.add(createKV(key, sb.toString(), it.hasNext()));
        sb.setLength(0);
        if (numSplits > REPORT_EXCEED_SPLITS) PigStatusReporter.getInstance()
            .getCounter("DCC Dynamic Exporter", (String) key.get(0)).increment(1);
        reporter.progress();
        n = 0;
        ++numSplits;
      }
      n++;
    }
    if (sb.length() != 0) {
      output.add(createKV(key, sb.toString(), false));
    }
    return output;
  }

  private Tuple createKV(Tuple key, String tsv, boolean more) throws ExecException {
    int state =
        isBegin ? (more ? STREAM_STATE.START.id() : STREAM_STATE.END.id()) : (more ? STREAM_STATE.SYNC.id() : STREAM_STATE.END
            .id());
    isBegin = false;
    DataByteArray bytes = new DataByteArray(tsv);
    String idx = StringUtils.replaceChars((String) key.get(0), '.', '_') + KEY_SEPARATOR +
        StringUtils.replaceChars((String) key.get(1), '.', '_') + KEY_SEPARATOR +
        StringUtils.replaceChars((String) key.get(2), '.', '_') + KEY_SEPARATOR +
        StringUtils.replaceChars((String) key.get(3), '.', '_') + KEY_SEPARATOR +
        state + KEY_SEPARATOR + bytes.size();

    Tuple content = TupleFactory.getInstance().newTuple(2);
    content.set(0, idx);
    content.set(1, bytes);
    return content;
  }

  @Override
  public Schema outputSchema(Schema input) {
    checkArgument(input.size() == 2, "Input should have 2 fields: (key,value)");
    try {
      checkArgument(input.getField(0).schema.size() == 4,
          "Key should have 4 fields but actual: "
              + input.getField(0).schema.toString());

      checkArgument(input.getField(1).schema.size() == 1,
          "Value should have 1 field but actual: "
              + input.getField(1).schema.toString());

      checkArgument(input.getField(1).type == DataType.BAG,
          "Value should have a bag type field but actual: "
              + input.getField(1).toString());

      UDFContext context = UDFContext.getUDFContext();
      Properties udfProp = context.getUDFProperties(this.getClass());

      StringBuilder sb = new StringBuilder();
      for (FieldSchema field : input.getField(1).schema.getField(0).schema.getFields()) {
        sb.append(field.alias);
        sb.append(",");
      }
      sb.setLength(sb.length() - 1);
      String headerline = sb.toString();
      udfProp.setProperty("headerline", headerline);

    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }
    List<FieldSchema> outputFields = newArrayListWithCapacity(2);
    // DataType.BAG
    outputFields.add(new FieldSchema(null, DataType.CHARARRAY));
    outputFields.add(new FieldSchema(null, DataType.BYTEARRAY));
    Schema recordSchema = new Schema(outputFields);
    try {
      return new Schema(new FieldSchema(null, recordSchema, DataType.BAG));
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
}