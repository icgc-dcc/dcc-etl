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

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.etl.indexer.factory.TransportClientFactory.newTransportClient;

import java.util.List;

import lombok.Cleanup;
import lombok.val;

import org.icgc.dcc.common.core.model.IndexType;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class IndexServiceTest {

  @Test
  public void testMappingWellformedness() {
    @Cleanup
    val service = new IndexService(newTransportClient("es://localhost:9300"));
    val types = ImmutableList.copyOf(DocumentType.values());

    service.initializeIndex("test-index", types);
  }

  @Test
  public void testMappingAvailability() {
    val documentTypeCount = getDocumentTypes();
    val indexTypeCount = getIndexTypes();
    assertThat(indexTypeCount).containsOnlyElementsOf(documentTypeCount);
  }

  private static List<String> getDocumentTypes() {
    return stream(DocumentType.values())
        .map(dt -> dt.getName())
        .collect(toImmutableList());
  }

  private static List<String> getIndexTypes() {
    return stream(IndexType.values())
        .map(dt -> dt.getName())
        .collect(toImmutableList());
  }

}
