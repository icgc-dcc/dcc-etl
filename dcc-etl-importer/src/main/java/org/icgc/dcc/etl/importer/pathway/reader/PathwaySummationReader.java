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
package org.icgc.dcc.etl.importer.pathway.reader;

import static org.apache.commons.lang3.StringEscapeUtils.unescapeHtml4;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.net.URI;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.pathway.model.PathwaySummation;
import org.icgc.dcc.etl.importer.util.AbstractTsvMapReader;

import com.google.common.collect.ImmutableList;

@Slf4j
public class PathwaySummationReader extends AbstractTsvMapReader {

  /**
   * Constants.
   */
  private static final String REACTOME_ID = "reactome_id";
  private static final String REACTOME_NAME = "reactome_name";
  private static final String SUMMATION = "summation";
  private static final String[] FIELD_NAMES = {
      REACTOME_ID,
      REACTOME_NAME,
      SUMMATION
  };

  @SneakyThrows
  public Iterable<PathwaySummation> read(URI summationFile) {
    log.info("Reading summations from {}...", summationFile);

    val summations = ImmutableList.<PathwaySummation> builder();
    int counter = 0;
    for (val record : readRecords(FIELD_NAMES, summationFile.toURL().openStream())) {
      val summation = PathwaySummation.builder()
          .reactomeId(record.get(REACTOME_ID))
          .reactomeName(unescapeHtml4(record.get(REACTOME_NAME)))
          .summation(record.get(SUMMATION))
          .build();

      summations.add(summation);
      counter++;
    }

    log.info("Finished reading {} summations", formatCount(counter));

    return summations.build();
  }

}
