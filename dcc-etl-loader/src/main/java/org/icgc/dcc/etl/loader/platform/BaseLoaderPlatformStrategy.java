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
package org.icgc.dcc.etl.loader.platform;

import static org.icgc.dcc.common.core.model.ClinicalType.CLINICAL_CORE_TYPE;
import static org.icgc.dcc.common.core.util.Joiners.EXTENSION;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.addMRFirstPartSuffix;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.duFile;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.isDirectory;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.lsAll;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.mkdirs;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.recursivelyDeleteDirectoryIfExists;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.rmr;
import static org.icgc.dcc.common.hadoop.util.HadoopConstants.DISABLED_SPECULATIVE_EXECUTION;
import static org.icgc.dcc.common.hadoop.util.HadoopConstants.ENABLED_COMPRESSION;
import static org.icgc.dcc.common.hadoop.util.HadoopConstants.MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION;
import static org.icgc.dcc.common.hadoop.util.HadoopConstants.MAPRED_OUTPUT_COMPRESSION_CODE_PROPERTY_NAME;
import static org.icgc.dcc.common.hadoop.util.HadoopConstants.MAPRED_OUTPUT_COMPRESS_PROPERTY_NAME;

import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.cascading.CascadingContext;
import org.icgc.dcc.common.cascading.taps.MongoDbTap;
import org.icgc.dcc.common.core.dcc.DccConstants;
import org.icgc.dcc.common.core.json.Jackson;
import org.icgc.dcc.common.core.model.DataType;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.util.Extensions;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.etl.loader.core.ProvidedDataReleaseDigest;
import org.icgc.dcc.etl.loader.flow.SummaryCollector;
import org.icgc.dcc.etl.loader.mongodb.LoaderMongoDbScheme;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.property.ConfigDef;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientURI;

/**
 * {@link LoaderPlatformStrategy} implementation that provides suitable defaults for common members.
 */
@Slf4j
public abstract class BaseLoaderPlatformStrategy implements LoaderPlatformStrategy {

  protected static final String LOADER_SEPARATOR = DccConstants.INPUT_FILES_SEPARATOR;
  private static final String LOADER_INTERMEDIATE_FILES_EXTENSION = Extensions.TSV;

  /**
   * See {@link PlatformData}.
   */
  protected final PlatformData platformData;

  @Getter
  private final Map<String, Map<FileType, String>> inputFiles;

  protected BaseLoaderPlatformStrategy(
      @NonNull final FileSystem fileSystem, // TODO: remove need to provide FS here
      @NonNull final PlatformData platformData) {
    this.platformData = platformData;
    this.inputFiles = LoaderInput.getInputFiles(
        platformData.getFileSystem(),
        platformData.getParentInputDir(),
        platformData.getProjectKeys());
    log.info(Jackson.formatPrettyLog("Input files:", inputFiles));
  }

  protected abstract CascadingContext getCascadingContext();

  protected abstract Map<?, ?> augmentFlowProperties();

  @Override
  public FlowConnector getFlowConnector() {
    return getCascadingContext()
        .getConnectors()
        .getFlowConnector(
            augmentFlowProperties());
  }

  @Override
  public void initialise(@NonNull final ProvidedDataReleaseDigest dataDigest) {
    val fileSystem = platformData.getFileSystem();

    log.info("Re-creating directory structure under: '{}'", platformData.getOutputDir());
    mkdirs(
        fileSystem,
        recursivelyDeleteDirectoryIfExists(
            fileSystem,
            new Path(platformData.getOutputDir())));
    createWorkingDirs(fileSystem, dataDigest.getProjectKeys());
    createOutputDirs(fileSystem, dataDigest.getFeatureTypes());
  }

  /**
   * Create working directories for each submission.
   */
  private void createWorkingDirs(
      @NonNull final FileSystem fileSystem,
      @NonNull final Set<String> projectKeys) {

    for (val projectKey : projectKeys) {
      val projectWorkingDir = getProjectWorkingDir(projectKey);
      log.info("Creating project working dir '{}'", projectWorkingDir);
      mkdirs(fileSystem, projectWorkingDir);
    }
  }

  /**
   * Create output directories at the release level and for each present type, irrespective of which submission it's in.
   */
  private void createOutputDirs(
      @NonNull final FileSystem fileSystem,
      @NonNull final Set<FeatureType> featureTypes) {

    for (val dataTypeDirPath : getDataTypeDirs(featureTypes)) {
      log.info("Creating output dir '{}'", dataTypeDirPath);
      mkdirs(fileSystem, dataTypeDirPath);
    }
  }

  @Override
  public void finalise(ProvidedDataReleaseDigest dataDigest) {
    val fileSystem = platformData.getFileSystem();

    for (String projectKey : dataDigest.getProjectKeys()) {
      Path workingDir = getProjectWorkingDir(projectKey);
      log.info("Deleting working directory '{}' (content: {})",
          workingDir, lsAll(fileSystem, workingDir));
      rmr(fileSystem, workingDir);
    }
  }

  @Override
  public Tap<?, ?, ?> getSourceTap(
      @NonNull final String submission,
      @NonNull final FileType fileType) {
    return getCascadingContext()
        .getTaps()
        .getNoCompressionNoHeaderNonStrictTsv(
            getInputFilePath(submission, fileType));
  }

  /**
   * TextLine is sufficient for now, but we may use something more sophisticated in the future.
   * <p>
   * Note that the files are cleared before run, then brought together for all submissions at the end of the run.
   * <p>
   * This method is augmented in the hadoop strategy to account for compression.
   */
  @Override
  public Tap<?, ?, ?> getFileSystemTap(DataType type, String submission) {
    return getCascadingContext()
        .getTaps()
        .getLines(
            getFileSystemPath(type, submission),
            SinkMode.KEEP);
  }

  @Override
  public Tap<?, ?, ?> getSummaryTap(
      @NonNull final String projectKey,
      @NonNull final SummaryCollector.SummaryTapInfo summaryTap) {
    return getCascadingContext()
        .getTaps()
        .getNoCompressionTsvWithHeader(
            getIntermediateFilePath(
                projectKey,
                summaryTap.getFileName()),
            summaryTap.getSummaryFields());
  }

  /**
   * Returns a path as {@link String} for an intermediate file (typically used for {@link Flow}s to communicate over.
   */
  private String getIntermediateFilePath(
      @NonNull final String projectKey,
      @NonNull final String fileName) {
    return EXTENSION.join(
        new Path(
            getProjectWorkingDir(projectKey),
            fileName)
            .toUri()
            .getPath(),
        LOADER_INTERMEDIATE_FILES_EXTENSION);
  }

  @Override
  public Tap<?, ?, ?> getDatabaseTap(DataType dataType) {
    MongoClientURI mongoUri = platformData.getMongoUri();

    val mongoTap = new MongoDbTap(
        new LoaderMongoDbScheme(
            platformData.getSubmissionModel(),
            dataType,
            mongoUri.getDatabase()),
        mongoUri.toString());

    // Ensure no speculative execution takes place for the mappers (no reducers for this job) because this is not an
    // idempotent operation
    mongoTap.getStepConfigDef()
        .setProperty(
            ConfigDef.Mode.REPLACE,
            MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION,
            DISABLED_SPECULATIVE_EXECUTION);

    return mongoTap;
  }

  /**
   * TODO: try and combine with validator's equivalent. (DCC-996)
   */
  @Override
  @SneakyThrows
  public Fields getFileHeader(String submission, FileType fileType) {
    val fileSystem = platformData.getFileSystem();

    return new Fields(HadoopUtils.getFileHeader(
        fileSystem,
        getHeaderContainingFilePath(
            fileSystem,
            getInputFilePath(
                submission,
                fileType)),
        LOADER_SEPARATOR));
  }

  protected String getInputFilePath(String submission, FileType fileType) {
    return inputFiles.get(submission).get(fileType);
  }

  @Override
  public final Path getDataTypeOutputDir(@NonNull final DataType dataType) {
    return new Path(
        platformData.getOutputDir(),
        dataType.getId());
  }

  /**
   * Path to which documents will be written to in a per {@link DataType} then per project key basis (cleared/recreated
   * before every run in {@link #initialise()}).
   */
  protected String getFileSystemPath(
      @NonNull final DataType dataType,
      @NonNull final String projectKey) {
    return new Path(
        getDataTypeOutputDir(dataType),
        projectKey)
        .toUri().toString();
  }

  /**
   * Returns a {@link Path} for the working directory of a given submission.
   */
  private Path getProjectWorkingDir(@NonNull final String projectKey) {
    return new Path(
        platformData.getOutputDir(),
        projectKey);
  }

  @SneakyThrows
  @Override
  public long size(String submission) {
    long sum = 0;
    for (String filePath : inputFiles.get(submission).values()) {
      sum += duFile(platformData.getFileSystem(), new Path(filePath));
    }
    return sum;
  }

  /**
   * Sets up properties related to output compression.
   * <p>
   * TODO: move to cascading abstraction
   */
  protected void setOutputCompression(ConfigDef configDef, String codec) {
    log.info("Setting {} output compression {}", codec);

    configDef.setProperty(
        ConfigDef.Mode.REPLACE,
        MAPRED_OUTPUT_COMPRESS_PROPERTY_NAME,
        ENABLED_COMPRESSION);
    configDef.setProperty(
        ConfigDef.Mode.REPLACE,
        MAPRED_OUTPUT_COMPRESSION_CODE_PROPERTY_NAME,
        codec);
  }

  /**
   * Can handle a regular file (e.g. ssm_p.txt) or a M/R directory output (e.g. ssm_p.txt/part-00000)
   * <p>
   * TODO: handle compression for part file suffix
   */
  private static String getHeaderContainingFilePath(FileSystem fileSystem, String inputFilePath) {
    return isDirectory(
        fileSystem,
        new Path(inputFilePath)) ? addMRFirstPartSuffix(inputFilePath) : inputFilePath;
  }

  private Iterable<Path> getDataTypeDirs(@NonNull final Set<FeatureType> featureTypes) {
    val builder = new ImmutableList.Builder<Path>()
        .add(getDataTypeOutputDir(CLINICAL_CORE_TYPE)); // Always provided
    for (val featureType : featureTypes) {
      builder.add(getDataTypeOutputDir(featureType));
    }

    return builder.build();
  }
}
