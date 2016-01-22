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
package org.icgc.dcc.etl.indexer.io;

import static org.icgc.dcc.etl.indexer.factory.JongoFactory.newJongo;
import static org.icgc.dcc.etl.indexer.model.CollectionFields.DEFAULT_COLLECTION_FIELDS;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("Useful for debugging only")
public class DirectMemoryMongoDBCollectionReaderTest {

  @Test
  @SneakyThrows
  @SuppressWarnings("deprecation")
  public void testRead() {
    @Cleanup
    val reader = new DirectMemoryMongoDBCollectionReader(newJongo("mongodb://localhost/etl-test-release17-0-0"));
    for (@SuppressWarnings("unused")
    val value : reader.readObservations(DEFAULT_COLLECTION_FIELDS)) {
      // No-op
    }
  }

}
