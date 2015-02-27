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
package org.icgc.dcc.etl.service;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static java.lang.Boolean.valueOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.icgc.dcc.common.core.util.EtlConventions.getIndexerOutputDir;
import static org.icgc.dcc.common.core.util.EtlConventions.getLoaderOutputDir;
import static org.icgc.dcc.common.core.util.FormatUtils.formatDuration;
import static org.icgc.dcc.common.core.util.FormatUtils.formatPercent;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.common.core.util.Protocol.FILE;
import static org.icgc.dcc.common.core.util.URIs.LOCAL_ROOT;
import static org.icgc.dcc.common.core.util.URLs.getUrlFromPath;
import static org.icgc.dcc.etl.indexer.factory.DocumentServiceFactory.newDistributedService;
import static org.icgc.dcc.etl.indexer.factory.DocumentServiceFactory.newLocalService;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.Min;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.hibernate.validator.constraints.NotEmpty;
import org.icgc.dcc.common.client.api.ICGCClientConfig;
import org.icgc.dcc.common.core.model.Identifiable;
import org.icgc.dcc.etl.core.config.EtlConfig;
import org.icgc.dcc.etl.importer.Importer;
import org.icgc.dcc.etl.indexer.core.Config;
import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.icgc.dcc.etl.loader.factory.LoaderServiceFactory;
import org.icgc.dcc.etl.loader.service.LoaderService;
import org.icgc.dcc.etl.stats.StatsService;
import org.icgc.dcc.etl.summarizer.service.SummarizerService;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;

@Slf4j
@RequiredArgsConstructor
public class EtlService {

  /**
   * First release number ever processed with the current ETL.
   */
  private static final int MINIMUM_RELEASE_NUMBER = 14;

  private final EtlConfig config;

  private int step = 0;

  private final Map<EtlAction, Stopwatch> durations = newLinkedHashMap();

  public void execute(
      @NonNull final String jobId,
      @NonNull @NotEmpty final String releasePrefix,
      @Min(value = MINIMUM_RELEASE_NUMBER) final int releaseNumber,
      @Min(value = 0) final int patchNumber,
      @Min(value = 0) final int runNumber,
      @NonNull final String dictionaryFilePath,
      @NonNull final String codeListsFilePath,
      @NonNull final Set<String> projectKeys,
      @NonNull final String workingDir,
      @NonNull final List<String> processNames,
      @NonNull final List<DocumentType> documentTypes,
      @NonNull final Optional<String> alias) {

    boolean all = processNames.isEmpty();
    boolean imports = all || EtlAction.IMPORT.in(processNames);
    boolean load = all || EtlAction.LOAD.in(processNames);
    boolean summarize = all || EtlAction.SUMMARIZE.in(processNames);
    boolean index = all || EtlAction.INDEX.in(processNames);
    boolean stats = all || EtlAction.STATS.in(processNames);

    val releaseName = getReleaseName(releasePrefix, releaseNumber);

    Stopwatch etlWatch = Stopwatch.createStarted();

    if (load) {
      logBanner("Loading...");
      Stopwatch watch = Stopwatch.createStarted();
      load(jobId, dictionaryFilePath, codeListsFilePath, projectKeys, workingDir, getLoaderOutputDir(workingDir));

      durations.put(
          EtlAction.LOAD,
          reportComponentDuration(EtlAction.LOAD, watch));
    }

    if (imports) {
      logBanner("Importing...");
      Stopwatch watch = Stopwatch.createStarted();
      imports(jobId, projectKeys);

      durations.put(
          EtlAction.IMPORT,
          reportComponentDuration(EtlAction.IMPORT, watch));
    }

    if (summarize) {
      logBanner("Summarizing...");
      Stopwatch watch = Stopwatch.createStarted();
      summarize(jobId, releaseName);

      durations.put(
          EtlAction.SUMMARIZE,
          reportComponentDuration(EtlAction.SUMMARIZE, watch));
    }

    if (index) {
      logBanner("Indexing...");
      Stopwatch watch = Stopwatch.createStarted();
      index(jobId, releaseName, documentTypes, alias, getIndexerOutputDir(workingDir));

      durations.put(
          EtlAction.INDEX,
          reportComponentDuration(EtlAction.INDEX, watch));
    }

    if (stats) {
      logBanner("Gathering statistics...");
      Stopwatch watch = Stopwatch.createStarted();
      stats(jobId, releaseName, workingDir, projectKeys);

      durations.put(
          EtlAction.STATS,
          reportComponentDuration(EtlAction.STATS, watch));
    }

    reportDuration(durations, etlWatch);
  }

  private void load(
      @NonNull final String releaseName,
      @NonNull final String dictionaryFilePath,
      @NonNull final String codeListsFilePath,
      @NonNull final Set<String> projectKeys,
      @NonNull final String parentInputDir,
      @NonNull final String outputDir) {
    LoaderService service = LoaderServiceFactory.createService(
        config.getFsUrl(),
        config.getFileSystemOutputCompression(),
        config.getReleaseMongoUri(),
        config.getLoaderHadoop(),
        config.getIdentifierServiceUri(),
        config.isFilterAllControlled(),
        config.getLoaderMaxConcurrentFlows());

    service.loadRelease(
        releaseName, getUrlFromPath(dictionaryFilePath), getUrlFromPath(codeListsFilePath), projectKeys,
        parentInputDir, outputDir);
  }

  private void imports(String releaseName, Set<String> projects) {
    val importer = new Importer(
        config.getReleaseMongoUri(),
        config.getGeneMongoUri(),
        config.getCacheDir(),
        config.getFathmmPostgresqlUri(),
        createICGCConfig(config));

    importer.import_(releaseName, projects);
  }

  private void summarize(
      @NonNull final String jobId,
      @NonNull final String releaseName) {
    SummarizerService service = new SummarizerService(
        config.getReleaseMongoUri());

    service.summarize(jobId, releaseName);
  }

  @SneakyThrows
  private void index(
      @NonNull final String jobId,
      @NonNull final String releaseName,
      @NonNull final List<DocumentType> types,
      @NonNull final Optional<String> alias,
      @NonNull final String outputDir) {
    val indexName = getIndexName(jobId);
    val databaseName = jobId;
    val mongoUri = getMongoUri(config.getReleaseMongoUri(), databaseName);

    // The new service uses system properties for Hadoop configuration
    val fsUri = firstNonNull(
        config.getIndexerHadoop().get(FS_DEFAULT_NAME_KEY),
        LOCAL_ROOT);
    val local = fsUri.startsWith(FILE.getId());

    // Create the service
    val indexConfig = Config.builder()
        .releaseName(releaseName)
        .mongoUri(mongoUri)
        .esUri(config.getEsUri())
        .fsUri(fsUri)
        .indexName(indexName)
        .fastaFile(config.getFastaFile())
        .outputDir(outputDir)
        .exportVCF(config.isExportVCF())
        .hadoop(config.getIndexerHadoop())
        .build();

    // Index the documents
    @Cleanup
    val service = local ? newLocalService(indexConfig) : newDistributedService(indexConfig);
    service.writeDocuments(types);
  }

  private void stats(String jobId, String releaseName, String dataParentDir, Set<String> projectKeys) {
    StatsService service = new StatsService(
        jobId,
        releaseName,
        dataParentDir,
        projectKeys,
        config.getFsUrl(),
        // TODO: account for fsLoaderRoot (DCC-1213)
        config.getReleaseMongoUri(),
        config.getEsUri());

    service.stats();
  }

  private void logBanner(String message) {
    log.info("{}", repeat("-", 90));
    log.info("[{}] {}", ++step, message);
    log.info("{}", repeat("-", 90));
  }

  private Stopwatch reportComponentDuration(EtlAction etlAction, Stopwatch watch) {
    Stopwatch duration = watch.stop();
    log.info("{} took: {}", etlAction.name(), formatDuration(duration));
    return duration;
  }

  private void reportDuration(Map<EtlAction, Stopwatch> durations, Stopwatch etlWatch) {
    Stopwatch fullDuration = etlWatch.stop();
    log.info("{}", repeat("-", 90));
    log.info("ETL took: {}", formatDuration(fullDuration));
    for (val entry : durations.entrySet()) {
      EtlAction etlAction = entry.getKey();
      Stopwatch componentDuration = entry.getValue();
      double componentDurationPercentage = componentDurationPercentage(componentDuration, fullDuration);

      log.info("  {}{} ({}%)", new Object[] {
          padEnd(etlAction + ":", 15, ' '),
          formatDuration(componentDuration),
          formatPercent(componentDurationPercentage)
      });
    }
  }

  private double componentDurationPercentage(Stopwatch componentDuration, Stopwatch fullDuration) {
    double full = fullDuration.elapsed(SECONDS);
    double component = componentDuration.elapsed(SECONDS);
    return full == 0.0 ?
        0.0 :
        component / full * 100.0;
  }

  public enum EtlAction implements Identifiable {
    IMPORT,
    LOAD,
    SUMMARIZE,
    INDEX,
    STATS;

    public boolean in(List<String> processNames) {
      return processNames.contains(this.name().toLowerCase());
    }

    @Override
    public String getId() {
      return name().toLowerCase();
    }

  }

  private static String getMongoUri(String mongoUri, String databaseName) {
    return PATH.join(mongoUri, databaseName);
  }

  private static ICGCClientConfig createICGCConfig(EtlConfig config) {
    return ICGCClientConfig.builder()
        .cgpServiceUrl(config.getIcgc().get("cgpServiceUrl"))
        .consumerKey(config.getIcgc().get("consumerKey"))
        .consumerSecret(config.getIcgc().get("consumerSecret"))
        .accessToken(config.getIcgc().get("accessToken"))
        .accessSecret(config.getIcgc().get("accessSecret"))
        .strictSSLCertificates(valueOf(config.getIcgc().get("strictSSLCertificates")))
        .requestLoggingEnabled(valueOf(config.getIcgc().get("requestLoggingEnabled")))
        .build();
  }

  private static String getIndexName(String jobId) {
    // See DCC-2473: Avoid ES error - "Invalid index name [ICGC17-0-8], must be lowercase"
    return jobId.toLowerCase();
  }

  private static String getReleaseName(String releasePrefix, int releaseNumber) {
    return releasePrefix + releaseNumber;
  }

}
