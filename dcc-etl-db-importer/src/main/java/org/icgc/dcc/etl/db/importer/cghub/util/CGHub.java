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
package org.icgc.dcc.etl.db.importer.cghub.util;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.find;
import static java.util.regex.Pattern.compile;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import lombok.SneakyThrows;
import lombok.val;

import org.icgc.dcc.common.core.util.Splitters;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.base.Predicate;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

public class CGHub {

  static String DISEASE_ABBREVIATION = "disease_abbr";

  static String RESULT = "Result";
  static String QUERY = "Query";
  static String HITS = "Hits";

  static String FILES = "files";

  static Pattern INTEGER_PATTERN = compile("^\\d+$");

  public static void main(String[] args) {
    // new CGHubImporter("mongodb://localhost/cghub", new File("/tmp/cghub")).execute();
    process();
  }

  private static void process() {
    val factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true); // default value from JAXP 1.0 was defined to be false.
    // May validate, use schemas, ... (see 140609160124.java)
    val document = getDocument(
        getDocumentBuilder(factory),
        "/tmp/cghub/BLCA.xml");

    val l = getChildElements(document);
    checkState(l.size() == 1, "TODO");

    Element documentElement = document.getDocumentElement();

    val childElements = getChildElements(documentElement);

    Node resultSet = document.getFirstChild();
    val nl2 = resultSet.getChildNodes();

    getDiseaseName(nl2);

    // TODO: check the "id=" part too
    int c = 0;
    for (int i = 0; i < nl2.getLength(); i++) {
      Node item = nl2.item(i);
      if (RESULT.equals(item.getNodeName())) {
        val record = processResultNode(item);
        System.out.println(record);
        c++;
      }
    }

    checkState(c == getHits(childElements), "TODO");
  }

  private static void getDiseaseName(NodeList nl) {
    for (int i = 0; i < nl.getLength(); i++) {
      Node item = nl.item(i);
      if (QUERY.equals(item.getNodeName())) {
        String textContent = item.getTextContent();
        Iterable<String> split = Splitters.COLON.split(textContent);
        Iterator<String> iterator = split.iterator();
        checkState(iterator.hasNext() && DISEASE_ABBREVIATION.equals(iterator.next()) && iterator.hasNext(), "TODO",
            DISEASE_ABBREVIATION);
        val diseaseName = iterator.next();

        checkState("BLCA".equals(diseaseName), "TODO");
        checkState(!iterator.hasNext(), "TODO");
      }
    }
  }

  private static DBObject processResultNode(Node resultNode) {
    val builder = new BasicDBObjectBuilder();

    val childElements = getChildElements(resultNode);
    checkState(!childElements.isEmpty(), "TODO");
    for (val childElement : childElements) {
      val subChildElements = getChildElements(childElement); // Possibly empty
      checkState((isFilesNode(childElement) && (subChildElements.size() == 1 || subChildElements.size() == 2))
          || subChildElements.isEmpty());
      val nodeName = childElement.getNodeName();
      val textContent = childElement.getTextContent();

      if ("analysis_id".equals(nodeName)) {
        builder.add("raw_data_accession", textContent);
      }

      if ("alicot_id".equals(nodeName)) {
        builder.add("alicot_id", textContent);
      }
      builder.add("repository", "CGHub");
    }

    return builder.get();
  }

  private static boolean isFilesNode(final org.w3c.dom.Node child) {
    return FILES.equals(child.getNodeName());
  }

  private static int getHits(List<Node> nodes) {
    val text = find(
        nodes,
        new Predicate<Node>() {

          @Override
          public boolean apply(Node node) {
            return HITS.equals(node.getNodeName());
          }

        }).getTextContent();
    checkState(isPositiveInteger(text));
    return Integer.parseInt(text);
  }

  private static List<Node> getChildElements(Node node) {

    val l = new ArrayList<Node>();
    if (node.hasChildNodes()) { // TODO: necessary?
      val cn = node.getChildNodes();
      for (int i = 0; i < cn.getLength(); i++) {
        val child = cn.item(i);
        if (child.getNodeType() == Node.ELEMENT_NODE) { // TODO: guava
          l.add(child);
        }
      }
    }

    return l;
  }

  @SneakyThrows
  private static DocumentBuilder getDocumentBuilder(final javax.xml.parsers.DocumentBuilderFactory factory) {
    return factory.newDocumentBuilder();
  }

  @SneakyThrows
  private static Document getDocument(DocumentBuilder builder, String path) {
    return builder.parse(new File(path));
  }

  private static boolean isPositiveInteger(String text) {
    return INTEGER_PATTERN.matcher(text).matches();
  }

}
