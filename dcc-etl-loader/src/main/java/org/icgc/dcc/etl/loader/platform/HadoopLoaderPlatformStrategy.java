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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static org.icgc.dcc.common.cascading.util.CascadingProperties.enableDotExports;
import static org.icgc.dcc.common.core.util.VersionUtils.getCommitId;
import static org.icgc.dcc.common.core.util.VersionUtils.getScmInfo;
import static org.icgc.dcc.common.hadoop.util.HadoopConstants.LZO_CODEC_PROPERTY_VALUE;
import static org.icgc.dcc.common.hadoop.util.HadoopProperties.enableIntermediateMapOutputCompression;
import static org.icgc.dcc.common.hadoop.util.HadoopProperties.enableJobOutputCompression;
import static org.icgc.dcc.common.hadoop.util.HadoopProperties.setAvailableCodecs;

import java.util.Map;
import java.util.Map.Entry;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.common.cascading.CascadingContext;
import org.icgc.dcc.common.core.model.DataType;
import org.icgc.dcc.common.hadoop.util.HadoopProperties;
import org.icgc.dcc.etl.loader.cascading.TupleEntrySerialization;
import org.icgc.dcc.etl.loader.service.LoaderService;

import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tuple.hadoop.TupleSerializationProps;

@Slf4j
public class HadoopLoaderPlatformStrategy extends BaseLoaderPlatformStrategy {

  private final Map<String, String> properties;

  public HadoopLoaderPlatformStrategy(
      @NonNull final Map<String, String> properties,
      @NonNull final FileSystem fileSystem,
      @NonNull final PlatformData platformData) {
    super(fileSystem, platformData);
    this.properties = checkNotNull(properties);
  }

  @Override
  protected CascadingContext getCascadingContext() {
    return CascadingContext.getDistributed();
  }

  /**
   * TODO: bring together with submission's counterpart in the submitter (create a dcc-hadoop module).
   */
  @Override
  protected Map<?, ?> augmentFlowProperties() {
    Map<Object, Object> flowProperties = newHashMap();

    // Register custom serializer that is able to serialize nested tuples / tuple entries
    TupleSerializationProps
        .addSerialization(
            flowProperties,
            TupleEntrySerialization.class.getName());

    // Pass through configuration and allow for overrides
    flowProperties.putAll(properties);

    flowProperties =
        enableDotExports(
        enableJobOutputCompression(
            enableIntermediateMapOutputCompression(
                setAvailableCodecs(flowProperties),
                LZO_CODEC_PROPERTY_VALUE),
            LZO_CODEC_PROPERTY_VALUE));

    HadoopProperties.setHadoopUserNameProperty();

    // M/R job entry point
    AppProps.setApplicationJarClass(flowProperties, LoaderService.class);
    AppProps.setApplicationName(flowProperties, "dcc-etl-loader");
    AppProps.setApplicationVersion(flowProperties, getCommitId());
    for (Entry<String, String> entry : getScmInfo().entrySet()) {
      AppProps.addApplicationTag(flowProperties, entry.toString());
    }

    return flowProperties;
  }

  @Override
  public Tap<?, ?, ?> getFileSystemTap(DataType type, String submission) {
    val fileSystemTap = super.getFileSystemTap(type, submission);

    // Enable compression for this tap only
    val compression = platformData.getFileSystemOutputCompression();
    if (compression.isEnabled()) {
      log.info("Setting output compressing: '{}'", compression.getCodec());
      setOutputCompression(
          fileSystemTap.getStepConfigDef(),
          compression.getCodec());
    } else {
      log.info("No filesystem output compression specified");
    }

    return fileSystemTap;
  }

}
