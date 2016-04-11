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
package org.icgc.dcc.etl.summarizer.service;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DONOR_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_SET_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.MUTATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.RELEASE_COLLECTION;
import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.common.core.util.Formats.formatDuration;

import java.net.UnknownHostException;

import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.etl.summarizer.core.DonorSummarizer;
import org.icgc.dcc.etl.summarizer.core.GeneSetSummarizer;
import org.icgc.dcc.etl.summarizer.core.GeneSummarizer;
import org.icgc.dcc.etl.summarizer.core.ProjectSummarizer;
import org.icgc.dcc.etl.summarizer.core.ReleaseSummarizer;
import org.icgc.dcc.etl.summarizer.repository.ReleaseRepository;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import com.google.common.base.Stopwatch;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

@Slf4j
@Data
public class SummarizerService {

  @NonNull
  private final String releaseMongoUri;

  public void summarize(@NonNull String jobId, @NonNull String releaseName) {
    val jongo = createJongo(jobId);

    // Resolve collections
    val projects = checkNotNull(collection(jongo, PROJECT_COLLECTION));
    val donors = checkNotNull(collection(jongo, DONOR_COLLECTION));
    val genes = checkNotNull(collection(jongo, GENE_COLLECTION));
    val observations = checkNotNull(collection(jongo, OBSERVATION_COLLECTION));
    val mutations = checkNotNull(collection(jongo, MUTATION_COLLECTION));
    val releases = checkNotNull(collection(jongo, RELEASE_COLLECTION));
    val geneSets = checkNotNull(collection(jongo, GENE_SET_COLLECTION));

    log.info("Collection '{}' has {} document(s)", projects.getName(), formatCount(projects.count()));
    log.info("Collection '{}' has {} document(s)", donors.getName(), formatCount(donors.count()));
    log.info("Collection '{}' has {} document(s)", genes.getName(), formatCount(genes.count()));
    log.info("Collection '{}' has {} document(s)", observations.getName(), formatCount(observations.count()));
    log.info("Collection '{}' has {} document(s)", mutations.getName(), formatCount(mutations.count()));
    log.info("Collection '{}' has {} document(s)", releases.getName(), formatCount(releases.count()));
    log.info("Collection '{}' has {} document(s)", geneSets.getName(), formatCount(geneSets.count()));

    // Create data source
    ReleaseRepository repository =
        new ReleaseRepository(genes, observations, donors, projects, mutations, releases, geneSets);

    execute(jobId, releaseName, repository);
  }

  private void execute(@NonNull String jobId, @NonNull String releaseName, @NonNull ReleaseRepository repository) {
    // Create collection summarizers
    val geneSetSummarizer = new GeneSetSummarizer(repository);
    val donorSummarizer = new DonorSummarizer(repository);
    val projectSummarizer = new ProjectSummarizer(repository);
    val geneSummarizer = new GeneSummarizer(repository);
    val releaseSummarizer = new ReleaseSummarizer(jobId, releaseName, repository);

    // For correctness, the aggregation order below must be respected:
    log.info("Starting summarizing...");
    Stopwatch cumulative = Stopwatch.createStarted();

    val delta = Stopwatch.createStarted();
    geneSetSummarizer.summarize();
    log.info("Summarizing 'gene set' took......... {}", formatDuration(delta));

    delta.reset().start();
    donorSummarizer.summarize();
    log.info("Summarizing 'donor' took......... {}", formatDuration(delta));

    delta.reset().start();
    projectSummarizer.summarize();
    log.info("Summarizing 'project' took....... {}", formatDuration(delta));

    delta.reset().start();
    geneSummarizer.summarize();
    log.info("Summarizing 'gene' took.......... {}", formatDuration(delta));

    delta.reset().start();
    releaseSummarizer.summarize();
    log.info("Summarizing 'release' took....... {}", formatDuration(delta));

    log.info("Finished summarizing in {}", formatDuration(cumulative));
  }

  private MongoCollection collection(Jongo jongo, ReleaseCollection releaseCollection) {
    return jongo.getCollection(releaseCollection.getId());
  }

  private DB db(String releaseUri) throws UnknownHostException {
    MongoClientURI uri = new MongoClientURI(releaseUri);
    MongoClient mongo = new MongoClient(uri);

    return mongo.getDB(uri.getDatabase());
  }

  private String releaseUri(@NonNull String jobId) {
    String separator = releaseMongoUri.charAt(releaseMongoUri.length() - 1) == '/' ? "" : "/";
    return releaseMongoUri + separator + jobId;
  }

  @SneakyThrows
  private Jongo createJongo(@NonNull final String jobId) {
    String releaseUri = releaseUri(jobId);

    log.info("Using connection: {}", releaseUri);
    DB db = db(releaseUri);

    return new Jongo(db);
  }

}
