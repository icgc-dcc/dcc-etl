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
package org.icgc.dcc.etl.indexer.service;

import static org.icgc.dcc.etl.indexer.factory.DocumentServiceFactory.newLocalService;
import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_TEXT_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.GENE_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.GENE_SET_TEXT_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.GENE_TEXT_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.MUTATION_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.MUTATION_TEXT_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.OBSERVATION_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.PROJECT_TEXT_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.PROJECT_TYPE;

import java.io.IOException;

import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class LocalDocumentServiceTest extends AbstractDocumentServiceTest {

  @Before
  public void setup() {
    this.service = newLocalService(config);
  }

  @Test
  public void testWriteDocuments() throws IOException {
    test(DocumentType.values());
  }

  @Test
  @Ignore
  public void testWriteTextDocuments() throws IOException {
    test(PROJECT_TEXT_TYPE, DONOR_TEXT_TYPE, GENE_TEXT_TYPE, MUTATION_TEXT_TYPE, GENE_SET_TEXT_TYPE);
  }

  @Test
  public void testWriteProjectDocuments() throws IOException {
    test(PROJECT_TYPE);
  }

  @Test
  @Ignore
  public void testWriteDonorDocuments() throws IOException {
    test(DONOR_TYPE);
  }

  @Test
  @Ignore
  public void testWriteDonorCentricDocuments() throws IOException {
    test(DONOR_CENTRIC_TYPE);
  }

  @Test
  @Ignore
  public void testWriteGeneCentricDocuments() throws IOException {
    test(GENE_CENTRIC_TYPE);
  }

  @Test
  @Ignore
  public void testWriteObservationCentricDocuments() throws IOException {
    test(OBSERVATION_CENTRIC_TYPE);
  }

  @Test
  public void testWriteMutationCentricDocuments() throws IOException {
    test(MUTATION_CENTRIC_TYPE);
  }

  @Test
  @Ignore
  public void testWriteDonorTextDocuments() throws IOException {
    test(DONOR_TEXT_TYPE);
  }

  @Test
  @Ignore
  public void testWriteGeneTextDocuments() throws IOException {
    test(GENE_TEXT_TYPE);
  }

  @Test
  @Ignore
  public void testWriteProjectTextDocuments() throws IOException {
    test(PROJECT_TEXT_TYPE);
  }

  @Test
  @Ignore
  public void testWriteMutationTextDocuments() throws IOException {
    test(MUTATION_TEXT_TYPE);
  }

}
