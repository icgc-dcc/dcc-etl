/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.stats;

import java.util.Set;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatsService {

  private final HdfsStats hdfsStats;
  private final MongoStats mongoStats;
  private final EsStats esStats;
  private final StatsResults results;

  public StatsService(
      @NonNull String jobId,
      @NonNull String releaseName,
      @NonNull String dataParentDir,
      @NonNull Set<String> projectKeys,
      @NonNull String hdfsUri,
      @NonNull String releaseMongoUri,
      @NonNull String esUri) {
    // Alias
    val indexName = jobId;
    val databaseName = jobId;

    hdfsStats = new HdfsStats(hdfsUri, dataParentDir, projectKeys);
    mongoStats = new MongoStats(releaseMongoUri, databaseName);
    esStats = new EsStats(esUri, indexName);
    results = new StatsResults();
  }

  @SneakyThrows
  public void stats() {
    // Gather statistics
    hdfsStats.stats(results);
    mongoStats.stats(results);
    esStats.stats(results);

    // Report statistics
    log.info("Results:\n{}", results.displayableResults());
    results.validateResults();
  }

}
