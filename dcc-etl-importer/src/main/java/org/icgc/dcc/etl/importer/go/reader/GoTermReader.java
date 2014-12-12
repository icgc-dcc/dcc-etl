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
package org.icgc.dcc.etl.importer.go.reader;

import static org.icgc.dcc.etl.importer.go.util.GoTermConverter.convertTermFrame;

import java.io.IOException;
import java.net.URL;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.go.model.GoTerm;
import org.obolibrary.oboformat.model.OBODoc;
import org.obolibrary.oboformat.parser.OBOFormatParser;
import org.obolibrary.oboformat.parser.OBOFormatParserException;

import com.google.common.collect.ImmutableSet;

@Slf4j
@RequiredArgsConstructor
public class GoTermReader {

  /**
   * Configuration.
   */
  @NonNull
  private final URL oboUrl;

  public Iterable<GoTerm> read() throws IOException, OBOFormatParserException {
    log.info("Reading OBO document from {}...", oboUrl);
    val document = readDocument();
    log.info("Finished reading OBO document");

    val terms = ImmutableSet.<GoTerm> builder();
    for (val termFrame : document.getTermFrames()) {
      val term = convertTermFrame(termFrame);

      log.debug("Read term: {}", term);
      terms.add(term);
    }

    return terms.build();
  }

  private OBODoc readDocument() throws IOException, OBOFormatParserException {
    return new OBOFormatParser().parse(oboUrl.toExternalForm());
  }

}
