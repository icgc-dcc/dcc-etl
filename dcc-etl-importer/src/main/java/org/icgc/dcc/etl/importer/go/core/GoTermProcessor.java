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
package org.icgc.dcc.etl.importer.go.core;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.importer.go.util.GoTermFilter.filterCurrentTerms;
import static org.icgc.dcc.etl.importer.go.util.GoTermFilter.filterObsoleteTerms;

import java.io.IOException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.go.model.GoTerm;
import org.icgc.dcc.etl.importer.go.reader.GoTermReader;
import org.obolibrary.oboformat.parser.OBOFormatParserException;

@Slf4j
@RequiredArgsConstructor
public class GoTermProcessor {

  /**
   * Dependencies.
   */
  @NonNull
  private final GoTermReader termReader;

  public Iterable<GoTerm> process() throws IOException, OBOFormatParserException {
    val timer = createStarted();

    log.info("Reading terms...");
    val terms = readTerms();

    log.info("Filter current terms...");
    val currentTerms = filterCurrentTerms(terms);

    log.info("Filtering obsolete terms...");
    val obsoleteTerms = filterObsoleteTerms(terms);

    log.info("Processed {} total terms, {} current, {} obsolete in {}.",
        formatCount(terms),
        formatCount(currentTerms),
        formatCount(obsoleteTerms), timer);

    return currentTerms;
  }

  private Iterable<GoTerm> readTerms() throws IOException, OBOFormatParserException {
    return termReader.read();
  }

}
