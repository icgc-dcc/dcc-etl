package org.icgc.dcc.etl.annotator.config;

import java.io.File;
import java.io.Serializable;

import lombok.Data;

import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.URL;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * SnpEff specific properties.<br>
 * The class is declared as a {@code @Component} to avoid serialization errors.
 * @see https://github.com/spring-projects/spring-boot/issues/1811
 */
@Data
@Component
@ConfigurationProperties("snpEff")
public class SnpEffProperties implements Serializable {

  /**
   * Configuration.
   */
  private File resourceDir;
  @URL
  private String resourceUrl;
  @NotBlank
  private String databaseVersion;
  @NotBlank
  private String referenceGenomeVersion;
  @NotBlank
  private String geneBuildVersion;

}
