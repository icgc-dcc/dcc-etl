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
package org.icgc.dcc.etl.indexer.repository;

import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
@RequiredArgsConstructor
public class ProjectRepository implements Closeable {

  @NonNull
  private final Jongo jongo;

  public int getTotalSsmTestedDonorCount() {
    val collection = getProjectCollection();
    val results = collection
        .aggregate("{ $group: { _id : null, sum : { $sum: '$_summary._ssm_tested_donor_count' } } }")
        .as(ObjectNode.class);

    checkState(!results.isEmpty(), "Could not calculate totalSsmTestedDonorCount");
    val totalSsmTestedDonorCount = results.get(0).get("sum").asInt();
    log.info("Calculated: totalSsmTestedDonorCount = {}", totalSsmTestedDonorCount);

    return totalSsmTestedDonorCount;
  }

  private MongoCollection getProjectCollection() {
    return jongo.getCollection(ReleaseCollection.PROJECT_COLLECTION.getId());
  }

  @Override
  public void close() throws IOException {
    jongo.getDatabase().getMongo().close();
  }

}
