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
package org.icgc.dcc.etl.loader.cascading;

import static cascading.tuple.Fields.ARGS;
import static cascading.tuple.Fields.REPLACE;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_DIGITAL_IMAGE_OF_STAINED_SECTION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SPECIMEN_TYPE;
import static org.icgc.dcc.common.core.util.FormatUtils._;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;

import java.net.URL;
import java.util.List;
import java.util.Set;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.hadoop.cascading.operation.BaseFunction;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This modifies the digital_image_of_stained_section field in donor specimen to a real URL, if applicable;
 */
@Slf4j
public class StainedSectionImageUpdate extends SubAssembly {

  private static final String IMG_URL = "http://cancer.digitalslidearchive.net/index_mskcc.php";
  private static final String SLIDE_LIST_URL =
      "http://cancer.digitalslidearchive.net/local_php/get_slide_list_from_db.php";
  private static final String TYPE_URL =
      "http://cancer.digitalslidearchive.net/local_php/datagroup_combo_connector.php";
  private static final String SLIDE_NAME_PARAMETER = "slide_name";
  private static final String NO_MATCH_REPLACEMENT_VALUE = null;

  private static final List<String> DISEASE_TYPES = getDiseasesType();
  private static final Set<String> LOOKUP = buildSlideImageLookup(DISEASE_TYPES);
  private static final Fields SPECIMEN_ID_FIELD =
      prefixedFields(SPECIMEN_TYPE, SUBMISSION_SPECIMEN_ID);
  private static final Fields DIGITAL_IMAGE_OF_STAINED_SECTION_FIELD =
      prefixedFields(SPECIMEN_TYPE, SUBMISSION_DIGITAL_IMAGE_OF_STAINED_SECTION);

  public StainedSectionImageUpdate(Pipe pipe) {
    setTails(new Each(
        pipe,
        SPECIMEN_ID_FIELD
            .append(DIGITAL_IMAGE_OF_STAINED_SECTION_FIELD),
        getFunction(),
        REPLACE));
  }

  private static Function<Void> getFunction() {
    return new BaseFunction<Void>(2, ARGS) {

      @Override
      public void operate(
          @SuppressWarnings("rawtypes") FlowProcess flowProcess,
          FunctionCall<Void> functionCall) {
        val specimenId = functionCall.getArguments().getString(SPECIMEN_ID_FIELD);

        functionCall
            .getOutputCollector()
            .add(new Tuple(
                specimenId,
                LOOKUP.contains(specimenId) ?
                    getMatch(specimenId) :
                    NO_MATCH_REPLACEMENT_VALUE));
      }

    };
  }

  private static String getMatch(
      @NonNull final String specimenId) {

    return _("%s?%s=%s", IMG_URL, SLIDE_NAME_PARAMETER, specimenId);
  }

  /**
   * Ported almost as-is from dchang's code.
   */
  @SneakyThrows
  private static Set<String> buildSlideImageLookup(List<String> typeList) {
    Set<String> lookup = newHashSet();

    for (String type : typeList) {
      SAXBuilder saxBuilder = new SAXBuilder();
      Document document = saxBuilder.build(new URL(SLIDE_LIST_URL + "?tumor_type=" + type));

      Element root = document.getRootElement();
      List<Element> items = root.getChildren("item");
      log.info("{} found {} items", type, formatCount(items));
      for (Element item : items) {
        String pyramidFilename = item.getChild("pyramid_filename").getText();

        if (isNullOrEmpty(pyramidFilename)) continue;

        // Chop of the last two parts to get sample/specimen
        String sample = pyramidFilename.substring(0, pyramidFilename.lastIndexOf("-"));
        sample = sample.substring(0, sample.lastIndexOf("-"));
        if (isNullOrEmpty(sample)) continue;
        lookup.add(sample);
      }
    }

    log.info("Lookup size: {}", lookup.size());
    return lookup;
  }

  /**
   * Ported almost as-is from dchang's code.
   */
  @SneakyThrows
  private static List<String> getDiseasesType() {
    List<String> result = newArrayList();
    SAXBuilder saxBuilder = new SAXBuilder();
    Document document = saxBuilder.build(new URL(TYPE_URL));

    Element root = document.getRootElement();
    List<Element> items = root.getChildren("option");
    for (Element item : items) {
      String val = item.getAttributeValue("value");
      result.add(val);
    }
    log.info("Found {} disease type", result);
    return result;
  }

}
