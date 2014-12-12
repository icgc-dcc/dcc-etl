package org.icgc.dcc.etl.annotator.config;

import static com.google.common.collect.Maps.newLinkedHashMap;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

import java.util.Map;

import lombok.val;

import org.icgc.dcc.etl.annotator.cascading.TapFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.property.AppProps;

import com.google.common.collect.ImmutableMap;

/**
 * Cascading specific configuration.
 */
@Configuration
public class CascadingConfig {

  /**
   * Dependencies.
   */
  @Value("#{hadoopProperties.properties}")
  private Map<Object, Object> properties;

  @Bean
  public TapFactory tapFactory() {
    return new TapFactory(isLocal());
  }

  @Bean
  public FlowConnector flowConnector() {
    // Adapt to configured platform
    return isLocal() ? new LocalFlowConnector(getProperties()) : new HadoopFlowConnector(getProperties());
  }

  private boolean isLocal() {
    return properties.get(FS_DEFAULT_NAME_KEY).toString().startsWith("file://");
  }

  private Map<Object, Object> getProperties() {
    // Mutable copy for modification
    val copy = newLinkedHashMap(properties);

    // Tell Cascading the jar to use when running within Hadoop
    AppProps.setApplicationJarClass(properties, this.getClass());

    return ImmutableMap.copyOf(copy);
  }

}
