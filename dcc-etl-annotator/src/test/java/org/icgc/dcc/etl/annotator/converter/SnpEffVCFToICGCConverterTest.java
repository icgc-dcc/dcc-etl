package org.icgc.dcc.etl.annotator.converter;

import static com.google.common.collect.ImmutableList.of;
import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;

import org.broadinstitute.variant.variantcontext.VariantContextBuilder;
import org.icgc.dcc.etl.annotator.model.AnnotatedFileType;
import org.icgc.dcc.etl.annotator.util.Alleles;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

@Ignore
public class SnpEffVCFToICGCConverterTest {

  /**
   * Class under test.
   */
  SnpEffVCFToICGCConverter converter = new SnpEffVCFToICGCConverter("75");

  // TODO: Create tests to cover all cases
  @Test
  public void testConvert() {
    val variant = new VariantContextBuilder()
        .chr("1")
        .start(1)
        .stop(1)
        .id("MU00001")
        // .genotypes(Genotypes)
        .attributes(ImmutableMap.<String, Object> of(SnpEffVCFToICGCConverter.INFO_EFF_FIELD, ""))
        .alleles(Alleles.createAlleles("A", of("T")))
        .make();

    val records = converter.convert(variant, AnnotatedFileType.SSM);

    // TODO: Verify results
    assertThat(records).isNotNull();
  }

}
