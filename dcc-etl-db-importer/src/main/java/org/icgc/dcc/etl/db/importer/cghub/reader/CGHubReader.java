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
package org.icgc.dcc.etl.db.importer.cghub.reader;

import static org.apache.commons.lang3.StringUtils.join;
import static org.icgc.dcc.etl.db.importer.cghub.model.CGHubProjects.getProjectId;
import static org.icgc.dcc.etl.db.importer.cghub.util.CGHubValues.getAnalyte;
import static org.icgc.dcc.etl.db.importer.cghub.util.CGHubValues.getSampleType;
import static org.icgc.dcc.etl.db.importer.cghub.util.CGHubXmls.PROJECT_ELEMENT_NAME;
import static org.icgc.dcc.etl.db.importer.cghub.util.CGHubXmls.RESULT_ELEMENT_NAME;
import static org.icgc.dcc.etl.db.importer.cghub.util.CGHubXmls.XPATHS;

import java.io.InputStream;
import java.util.Stack;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.XMLEvent;

import lombok.SneakyThrows;
import lombok.val;

import com.mongodb.BasicDBObject;

public class CGHubReader {

  /**
   * Resulting document property names.
   */
  private static final String ANALYTE_CODE_PROPERTY_NAME = "analyte_code";
  private static final String SAMPLE_TYPE_PROPERTY_NAME = "sample_type";
  private static final String REPOSITORY_PROPERTY_NAME = "repository";
  private static final String PROJECT_ID_PROPERTY_NAME = "_project_id";

  /**
   * The repository name that will be stored in the imported record.
   */
  private static final String SOURCE_REPOSITORY_NAME = "CGHub";

  @SneakyThrows
  public void parse(InputStream inputStream, DocumentCallback callback) {
    val eventReader = createEventReader(inputStream);
    val currentPath = new Stack<String>();

    BasicDBObject document = null;
    while (eventReader.hasNext()) {
      val event = eventReader.nextEvent();

      document = parseEvent(callback, eventReader, currentPath, document, event);
    }
  }

  private BasicDBObject parseEvent(DocumentCallback callback, XMLEventReader eventReader, Stack<String> currentPath,
      BasicDBObject document, XMLEvent event)
      throws XMLStreamException {
    if (event.isStartElement()) {
      val startElement = event.asStartElement();
      val elementName = startElement.getName().getLocalPart();
      currentPath.push(elementName);

      // Current path expressed in XPath
      if (elementName.equals(RESULT_ELEMENT_NAME)) {
        document = new BasicDBObject();
        return document;
      }

      if (isProcessable(currentPath)) {
        // Move forward to get the value of the current element
        event = eventReader.nextEvent();

        // When the current element is empty event will not be instanceof Characters
        String value = "Unknown"; // Initialize the value to string Unknown instead of null
        if (event instanceof Characters) {
          value = event.asCharacters().getData();
        }

        if (event instanceof EndElement) {
          currentPath.pop();
        }

        document.put(elementName, value);

        if (elementName.equals(PROJECT_ELEMENT_NAME)) {
          document.put(PROJECT_ID_PROPERTY_NAME, getProjectId(value));
          document.put(REPOSITORY_PROPERTY_NAME, SOURCE_REPOSITORY_NAME);
        }

        if (elementName.equals(SAMPLE_TYPE_PROPERTY_NAME)) {
          document.put(SAMPLE_TYPE_PROPERTY_NAME, getSampleType(value));
        }

        if (elementName.equals(ANALYTE_CODE_PROPERTY_NAME)) {
          document.put(ANALYTE_CODE_PROPERTY_NAME, getAnalyte(value));
        }

        return document;
      }
    }

    if (event.isEndElement()) {
      val endElement = event.asEndElement();
      currentPath.pop();

      if (endElement.getName().getLocalPart().equals(RESULT_ELEMENT_NAME)) {
        // Extension point
        callback.handle(document);
      }
    }

    return document;
  }

  private static XMLEventReader createEventReader(InputStream inputStream) throws XMLStreamException,
      FactoryConfigurationError {
    val factory = XMLInputFactory.newInstance();

    return factory.createXMLEventReader(inputStream);
  }

  private static boolean isProcessable(Stack<String> currentPath) {
    val absolutePath = "/" + join(currentPath, "/");

    return XPATHS.contains(absolutePath);
  }

  /**
   * A callback to perform an action on each parsed result record.
   */
  public static interface DocumentCallback {

    void handle(BasicDBObject document);

  }

}
