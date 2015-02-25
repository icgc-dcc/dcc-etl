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
package org.icgc.dcc.etl.loader.platform;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static org.icgc.dcc.common.core.model.FileTypes.FileSubType.PRIMARY_SUBTYPE;

import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import lombok.Builder;

import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FileTypes.FileSubType;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.model.SubmissionModel;
import org.icgc.dcc.etl.loader.core.ProvidedDataReleaseDigest;
import org.icgc.dcc.etl.loader.core.ProvidedDataSubmissionDigest;
import org.icgc.dcc.common.hadoop.util.HadoopCompression;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mongodb.MongoClientURI;

/**
 * Placeholder for data pertaining to {@link LoaderPlatformStrategy} objects.
 * <p>
 * TODO: revisit this class in the context of DCC-1876 (still needed? refocus?).
 */
@Builder
public class PlatformData {

  /**
   * The {@link FileSystem} abstraction to read/write files.
   */
  @Getter
  private final FileSystem fileSystem;

  /**
   * Default input directory.
   */
  @Getter
  private final String parentInputDir;

  /**
   * Output directory.
   */
  @Getter
  private final String outputDir;

  /**
   * Name of release under consideration.
   */
  @Getter
  private final String releaseName;

  /**
   * TODO
   */
  @Getter
  private final Set<String> projectKeys;

  /**
   * URI for the mongodb database.
   */
  @Getter
  private final MongoClientURI mongoUri;

  /**
   * Compression to use for the output of the exported files (for consumption by the downloader component's "exporter").
   */
  @Getter
  private final HadoopCompression fileSystemOutputCompression;

  /**
   * TODO
   */
  @Getter
  private final SubmissionModel submissionModel;

  /**
   * Returns a description of the file types available for the given release.
   * <p>
   * For instance:
   * <code>{project1: {ssm: {m, p}, cnsm: {m, p, s}, stsm: {m, p}, exp_seq: {m, g}, project2: {ssm: {m, p}, stsm: {m, p, s}}, project3: {meth_seq: {m, p}, mirna_seq: {m, p, s}}}<code>
   */
  public static ProvidedDataReleaseDigest summarizeReleaseDataProvided(Map<String, Map<FileType, String>> inputFiles) {

    val mapBuilder = new ImmutableMap.Builder<String, ProvidedDataSubmissionDigest>();
    for (val projectKey : inputFiles.keySet()) {
      mapBuilder.put(
          projectKey,
          summarizeSubmissionDataProvided(inputFiles.get(projectKey)));
    }

    return new ProvidedDataReleaseDigest()
        .setDataSubmissionDigests(mapBuilder.build());
  }

  /**
   * Returns a description of the file types available for the given project.
   */
  private static ProvidedDataSubmissionDigest summarizeSubmissionDataProvided(
      @NonNull final Map<FileType, String> projectFiles) {

    val mapBuilder = new ImmutableMap.Builder<FeatureType, Set<FileSubType>>();
    for (val featureType : FeatureType.values()) {
      if (hasFeatureType(projectFiles, featureType)) {
        val availableSubTypes = ImmutableSet.copyOf(
            transform(
                filter(
                    featureType.getCorrespondingFileTypes(),
                    in(projectFiles.keySet())),
                FileType.getGetSubTypeFunction()));
        checkConsistency(featureType, availableSubTypes);
        mapBuilder.put(featureType, availableSubTypes);
      }
    }

    return new ProvidedDataSubmissionDigest()
        .setDataTypes(mapBuilder.build());
  }

  /**
   * Determine if the target {@link FeatureType} is present in the supplied {@code submisssionDirectory} subject to the
   * {@code dictionary}.
   */
  private static boolean hasFeatureType(Map<FileType, String> projectFiles, FeatureType featureType) {
    return projectFiles.containsKey(featureType.getDataTypePresenceIndicator());
  }

  /**
   * At the very least "p" or "g" must be present, since "m" is necessarily present at this stage.
   */
  private static void checkConsistency(FeatureType featureType, Set<FileSubType> availableSubTypes) {
    checkState(availableSubTypes.contains(PRIMARY_SUBTYPE),
        "Inconsistent set of available sub-types found for '%s': '%s'",
        featureType, availableSubTypes);
  }

}
