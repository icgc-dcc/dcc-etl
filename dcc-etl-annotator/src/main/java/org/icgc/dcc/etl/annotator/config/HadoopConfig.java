package org.icgc.dcc.etl.annotator.config;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.etl.annotator.util.MapConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Hadoop specific configuration.
 */
@Configuration
public class HadoopConfig {

  /**
   * Dependencies.
   */
  @Autowired
  private HadoopProperties properties;

  @Bean
  public FileSystem fileSystem() throws IOException {
    return FileSystem.get(configuration());
  }

  private org.apache.hadoop.conf.Configuration configuration() {
    return new MapConfiguration(properties.getProperties());
  }

}
