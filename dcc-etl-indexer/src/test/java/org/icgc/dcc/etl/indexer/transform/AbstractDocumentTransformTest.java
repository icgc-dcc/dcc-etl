/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.icgc.dcc.etl.indexer.transform;

import static com.google.common.primitives.Ints.tryParse;
import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.icgc.dcc.etl.indexer.factory.JongoFactory.newJongo;
import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;
import static org.icgc.dcc.etl.indexer.model.DocumentType.OBSERVATION_CENTRIC_TYPE;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.client.transport.TransportClient;
import org.icgc.dcc.etl.indexer.core.Document;
import org.icgc.dcc.etl.indexer.core.DocumentContext;
import org.icgc.dcc.etl.indexer.core.DocumentTransform;
import org.icgc.dcc.etl.indexer.io.MongoDBCollectionReader;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.icgc.dcc.etl.indexer.util.DefaultDocumentContext;
import org.jongo.Jongo;
import org.json.JSONException;
import org.junit.Before;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;

@Slf4j
public abstract class AbstractDocumentTransformTest {

  /**
   * Test config.
   */
  static final String TEST_INDEX_NAME = "dcc-release-load-prod-07e-42-icgc15-feb07_2";
  static final String TEST_MONGO_URI = "mongodb://localhost";
  static final String TEST_MONGO_DATABASE_NAME = "dcc-release-load-prod-07e-42-ICGC15-feb07_2";
  static final String TEST_ES_URI = "es://localhost:9300";

  /**
   * Class under test.
   */
  protected DocumentTransform transform;

  /**
   * Collaborators.
   */
  protected DocumentContext context;
  protected MongoDBCollectionReader reader;
  protected TransportClient client;
  protected Jongo jongo;

  @Before
  public void setUp() {
    this.client = newTransportClient(TEST_ES_URI, false);
    this.reader = new MongoDBCollectionReader(newJongo(TEST_MONGO_URI + "/" + TEST_MONGO_DATABASE_NAME));
    this.context = new DefaultDocumentContext("test", getType(), reader);
    this.transform = getType().getTransform();
  }

  protected void compare() throws JSONException {
    int i = 0;

    val roots = readCollection();
    for (val root : roots) {
      val watch = Stopwatch.createStarted();
      val document = transform.transformDocument(root, context);
      log.info("{}. Created document in {}", ++i, watch);

      assertDocument(document);
    }
  }

  protected Iterable<ObjectNode> readCollection() {
    // Shorthands
    val collection = getType().getCollection();
    val fields = getType().getFields();

    return reader.read(collection, fields.getFields(collection));
  }

  protected abstract DocumentType getType();

  protected void assertDocument(Document document) throws JSONException {
    val expected = getExpectedSource(document);
    val actual = getActualSource(document);

    try {
      assertEquals(expected, actual, LENIENT);
    } catch (AssertionError e) {
      // Account for bug in previous implementation
      val message = e.getMessage();
      val pattern = Pattern.compile("gene\\[\\]: Expected (\\d+) values but got (\\d+)");
      val matcher = pattern.matcher(message);
      if (matcher.matches()) {
        val x = tryParse(matcher.group(1));
        val y = tryParse(matcher.group(2));

        if (y > 1 && x - y == 1) {
          return;
        }
      }

      log.info("Expected:    {}", expected);
      log.warn("Actual:      {}", actual);
      log.error("Difference: {}", message);

      throw e;
    }
  }

  protected String getActualSource(Document document) {
    return document.getSource().toString();
  }

  @SneakyThrows
  protected String getExpectedSource(@NonNull Document document) {
    if (document.getType() == OBSERVATION_CENTRIC_TYPE) {
      // No id
      return client
          .prepareSearch(TEST_INDEX_NAME)
          .setTypes(document.getType().getName())
          .setPostFilter(
              andFilter(
                  nestedFilter("ssm",
                      termFilter("_mutation_id", document.getSource().get("ssm").get("_mutation_id").asText())),
                  nestedFilter("donor",
                      termFilter("donor._donor_id", document.getSource().get("donor").get("_donor_id").asText()))))
          .execute()
          .get()
          .getHits()
          .getAt(0)
          .getSourceAsString().replaceFirst("\"_id\"[^,]+,", "");
    } else {
      // Use id
      return client.prepareGet(TEST_INDEX_NAME, document.getType().getName(), document.getId())
          .execute()
          .get()
          .getSourceAsString();
    }
  }

}
