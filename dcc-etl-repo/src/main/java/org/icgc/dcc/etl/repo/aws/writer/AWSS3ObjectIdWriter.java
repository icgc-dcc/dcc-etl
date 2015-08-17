/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.repo.aws.writer;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.util.Set;

import org.icgc.dcc.etl.repo.util.AbstractJongoWriter;
import org.jongo.MongoCollection;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClientURI;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSS3ObjectIdWriter extends AbstractJongoWriter<Set<String>> {

  /**
   * Dependencies.
   */
  @NonNull
  protected final MongoCollection awsCollection;

  public AWSS3ObjectIdWriter(MongoClientURI mongoUri) {
    super(mongoUri);
    this.awsCollection = jongo.getCollection("AWS");
  }

  @Override
  public void write(Set<String> objectIds) {
    log.info("Clearing object id documents...");
    clearObjectIds();

    log.info("Writing object id documents...");
    saveObjectIds(objectIds);
  }

  private void clearObjectIds() {
    log.info("Clearing '{}' documents in collection '{}'", awsCollection.getName());
    val result = awsCollection.remove("{}");
    checkState(result.getLastError().ok(), "Error clearing mongo: %s", result);

    log.info("Finished clearing {} documents in collection '{}'",
        formatCount(result.getN()), awsCollection.getName());
  }

  private void saveObjectIds(Set<String> objectIds) {
    for (val objectId : objectIds) {
      val document = ImmutableMap.of("objectId", objectId);
      awsCollection.save(document);
    }
  }

}
