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
package org.icgc.dcc.etl.loader.service;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.icgc.dcc.common.core.model.Configurations.FILTER_ALL_CONTROLLED;
import static org.icgc.dcc.common.core.model.Configurations.IDENTIFIER_CLIENT_CLASS_NAME_KEY;
import static org.icgc.dcc.common.core.model.Configurations.IDENTIFIER_KEY;
import static org.icgc.dcc.common.core.model.Configurations.LOADER_MAX_CONCURRENT_FLOWS;
import static org.icgc.dcc.common.core.model.Configurations.RELEASE_MONGO_URI_KEY;
import static org.icgc.dcc.common.core.util.Jackson.formatPrettyJson;
import static org.icgc.dcc.common.core.util.Joiners.INDENT;
import static org.icgc.dcc.etl.loader.mongodb.MongoDbProcessing.postProcessMongoDb;
import static org.icgc.dcc.submission.dictionary.util.Dictionaries.readCodeList;
import static org.icgc.dcc.submission.dictionary.util.Dictionaries.readDictionary;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Set;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.common.core.model.SubmissionModel;
import org.icgc.dcc.etl.loader.core.LoaderContext;
import org.icgc.dcc.etl.loader.core.LoaderJob;
import org.icgc.dcc.etl.loader.core.LoaderPlan;
import org.icgc.dcc.etl.loader.core.LoaderPlanner;
import org.icgc.dcc.etl.loader.core.ProvidedDataReleaseDigest;
import org.icgc.dcc.etl.loader.mongodb.MongoDbProcessing;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategy;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategyFactory;
import org.icgc.dcc.etl.loader.platform.PlatformData;
import org.icgc.dcc.etl.loader.service.LoaderModel.Submission;
import org.icgc.dcc.common.hadoop.util.HadoopCompression;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mongodb.MongoClientURI;

/**
 * Loads a release.
 */
@Slf4j
public class LoaderService {

  // TODO: Move to Configurations
  public static final String IDENTIFIER_AUTH_TOKEN_PARAM = "identifierAuthToken";

  @NonNull
  private final String releaseMongoUri;

  @NonNull
  private final FileSystem fileSystem;

  @NonNull
  private final LoaderPlatformStrategyFactory platformStrategyFactory;

  @NonNull
  private final HadoopCompression fileSystemOutputCompression;

  @NonNull
  private final String identifierClientClassName;
  @NonNull
  private final String identifierUri;
  private final String identifierAuthToken;
  private final boolean filterAllControlled;
  private final int maxConcurrentFlows;

  @Inject
  public LoaderService(
      @Named(RELEASE_MONGO_URI_KEY) String releaseMongoUri,

      HadoopCompression fileSystemOutputCompression,

      @Named(IDENTIFIER_CLIENT_CLASS_NAME_KEY) String identifierClientClassName,
      @Named(IDENTIFIER_KEY) String identifierUri,
      @Named(IDENTIFIER_AUTH_TOKEN_PARAM) String identifierAuthToken,

      @Named(FILTER_ALL_CONTROLLED) boolean filterAllControlled,
      @Named(LOADER_MAX_CONCURRENT_FLOWS) int maxConcurrentFlows,

      FileSystem fs, LoaderPlatformStrategyFactory platformStrategyFactory) {

    this.releaseMongoUri = checkNotNull(releaseMongoUri);
    this.fileSystem = checkNotNull(fs);
    this.platformStrategyFactory = checkNotNull(platformStrategyFactory);

    this.fileSystemOutputCompression = checkNotNull(fileSystemOutputCompression);

    this.identifierClientClassName = checkNotNull(identifierClientClassName);
    this.identifierUri = checkNotNull(identifierUri);
    this.identifierAuthToken = identifierAuthToken;

    this.filterAllControlled = filterAllControlled;
    this.maxConcurrentFlows = maxConcurrentFlows;
  }

  /**
   * Loads the specified release.
   */
  public void loadRelease(
      @NonNull final String jobId,
      @NonNull final URL dictionaryURL,
      @NonNull final URL codeListsURL,
      @NonNull final Set<String> projectKeys,
      @NonNull final String parentInputDir,
      @NonNull final String outputDir) {
    int stepNumber = 1;

    log.info("Loading release: '{}'", jobId);
    log.info("Dictionary file path: '{}'", dictionaryURL);
    log.info("Codelists file path: '{}'", codeListsURL);
    log.info("Parent input dir: '{}'", parentInputDir);
    log.info("Ouptput dir: '{}'", outputDir);
    log.info("Project keys: '{}'", projectKeys);
    log.info("Release mongo URI = {} ", releaseMongoUri);
    log.info("Identifier URI = {} ", identifierUri);

    // Get mongo client for the release database
    val releaseMongoClientURI = getReleaseMongoUri(jobId);

    // Get the submission context
    val dictionary = Submission.augmentDictionary(readDictionary(dictionaryURL));
    val submissionModel = SubmissionModelFactory.get(dictionary, readCodeList(codeListsURL));
    val context = getLoaderContext(jobId, submissionModel);

    // Build the placeholder for platform-related data
    val platformData = PlatformData.builder()
        .fileSystem(fileSystem)
        .releaseName(jobId)
        .parentInputDir(parentInputDir)
        .outputDir(outputDir)
        .projectKeys(projectKeys)
        .mongoUri(releaseMongoClientURI)
        .fileSystemOutputCompression(fileSystemOutputCompression)
        .submissionModel(submissionModel)
        .build();

    // Create platform strategy (for filesystem exports and mongo loads)
    val platformStrategy = platformStrategyFactory.get(platformData);

    // Indexes mongodb collections
    log.info(banner());
    log.info("[" + stepNumber++ + "] Adding indexes for '{}'...", jobId);
    log.info(banner());
    MongoDbProcessing.index(releaseMongoClientURI, jobId);
    log.info("Finished adding indexes for '{}'.", jobId);

    // Describe available data
    log.info(banner());
    log.info("[" + stepNumber++ + "] Summarizing data for '{}'...", jobId);
    log.info(banner());
    val dataDigest = PlatformData.summarizeReleaseDataProvided(platformStrategy.getInputFiles());
    log.info(INDENT.join("Finished summarizing data for '{}':", "'{}'."),
        jobId, formatPrettyJson(dataDigest));

    // Plan the load
    log.info(banner());
    log.info("[" + stepNumber++ + "] Planning load for '{}'...", jobId);
    log.info(banner());
    val plan = plan(context, platformStrategy, dataDigest, maxConcurrentFlows);
    log.info("Finished planning load for '{}'.", jobId);

    // Run the load plan
    log.info(banner());
    log.info("[" + stepNumber++ + "] Running load for '{}'...", jobId);
    log.info(banner());
    run(plan, context, releaseMongoClientURI, platformStrategy, maxConcurrentFlows);
    log.info("Finished running load for '{}'.", jobId);

    // Detect and fix mongodb duplicates (see
    // https://jira.oicr.on.ca/browse/DCC-1448?focusedCommentId=54493&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-54493)
    log.info(banner());
    log.info("[" + stepNumber++ + "] Post-processing load for '{}'...", jobId);
    log.info(banner());
    postProcessMongoDb(releaseMongoClientURI, jobId);
    log.info("Finished post-processing load for '{}'.", jobId);
  }

  private LoaderContext getLoaderContext(String releaseName, SubmissionModel submissionModel) {

    return new LoaderContext(
        submissionModel,
        identifierClientClassName,
        identifierUri,
        releaseName,
        identifierAuthToken,
        filterAllControlled);
  }

  /**
   * Creates a loader mongo db uri from a release name.
   */
  private MongoClientURI getReleaseMongoUri(String databaseName) {
    return new MongoClientURI(releaseMongoUri + "/" + databaseName);
  }

  private LoaderPlan plan(
      LoaderContext context, LoaderPlatformStrategy platformStrategy, ProvidedDataReleaseDigest dataDigest,
      int maxConcurrentFlows) {
    return new LoaderPlanner(context, platformStrategy, maxConcurrentFlows)
        .planRelease(dataDigest);
  }

  private void run(LoaderPlan plan,
      LoaderContext context, MongoClientURI releaseMongoClientURI, LoaderPlatformStrategy platformStrategy,
      int maxConcurrentFlows) {
    val job = new LoaderJob(context, platformStrategy, plan);
    job.run();
    job.logCollectionStats(releaseMongoClientURI);
  }

  @SneakyThrows
  public static <T> T forName(String className) {
    log.info("Creating instance for {}", className);
    Class<?> clazz = Class.forName(className);
    Constructor<?> constructor = clazz.getConstructor();
    @SuppressWarnings("unchecked")
    T t = (T) constructor.newInstance();
    return t;
  }

  private static String banner() {
    return repeat("-", 80);
  }

}
