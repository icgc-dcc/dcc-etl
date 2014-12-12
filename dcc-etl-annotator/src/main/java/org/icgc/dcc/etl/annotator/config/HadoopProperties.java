package org.icgc.dcc.etl.annotator.config;

import static com.google.common.collect.Maps.newLinkedHashMap;

import java.util.Map;

import javax.annotation.PostConstruct;

import lombok.Data;

import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Hadoop specific properties.
 */
@Data
@Configuration
@ConfigurationProperties("hadoop")
public class HadoopProperties {

  /**
   * Configuration.
   */
  @NotEmpty
  private String username;
  private Map<String, String> properties = newLinkedHashMap();

  @PostConstruct
  public void init() {
    // Required for access
    System.setProperty("HADOOP_USER_NAME", username);
  }

}
