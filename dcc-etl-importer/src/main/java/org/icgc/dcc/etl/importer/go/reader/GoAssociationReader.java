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

import static org.icgc.dcc.etl.importer.go.util.GoAssociationConverter.convertGeneAnnotation;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.go.model.GoAssociation;

import owltools.gaf.GafDocument;
import owltools.gaf.parser.GafObjectsBuilder;

import com.google.common.collect.ImmutableList;

@Slf4j
@RequiredArgsConstructor
public class GoAssociationReader {

  /**
   * Configuration.
   */
  @NonNull
  private final URL gafUrl;

  public Iterable<GoAssociation> read() throws IOException, URISyntaxException {
    log.info("Reading GAF document from {}...", gafUrl);
    val document = readDocument();
    log.info("Finished reading GAF document");

    val associations = ImmutableList.<GoAssociation> builder();
    for (val annotation : document.getGeneAnnotations()) {
      val association = convertGeneAnnotation(annotation);

      log.debug("Read association: {}", association);
      associations.add(association);
    }

    return associations.build();
  }

  private GafDocument readDocument() throws IOException, URISyntaxException {
    return new GafObjectsBuilder().buildDocument(gafUrl.toExternalForm());
  }

}
