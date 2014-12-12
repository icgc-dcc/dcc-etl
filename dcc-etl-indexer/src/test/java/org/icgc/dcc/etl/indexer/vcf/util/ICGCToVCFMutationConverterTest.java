package org.icgc.dcc.etl.indexer.vcf.util;

import static com.google.common.collect.ImmutableList.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl.indexer.vcf.model.MutationType.DELETION;
import static org.icgc.dcc.etl.indexer.vcf.model.MutationType.INSERTION;
import static org.icgc.dcc.etl.indexer.vcf.model.MutationType.SINGLE_BASE_SUBSTITUTION;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;

import lombok.RequiredArgsConstructor;
import lombok.val;
import net.sf.picard.reference.IndexedFastaSequenceFile;

import org.icgc.dcc.etl.indexer.vcf.model.MutationType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;

@RunWith(value = Parameterized.class)
@RequiredArgsConstructor
public class ICGCToVCFMutationConverterTest {

  /**
   * Test config.
   */
  private static final File REFERENCE_GENOME = new File("/tmp/GRCh37.fasta");

  /**
   * Class under test.
   */
  ICGCToVCFMutationConverter converter;

  /**
   * Input
   */
  final String chromosome;
  final long start;
  final long end;
  final String mutation;
  final MutationType type;
  final String reference;

  /**
   * Expected
   */
  final VCFMutation expected;

  @Before
  public void setUp() throws FileNotFoundException {
    this.converter = new ICGCToVCFMutationConverter(new IndexedFastaSequenceFile(REFERENCE_GENOME));
  }

  /**
   * Test data.
   * <p>
   * @see https://wiki.oicr.on.ca/display/DCCSOFT/Aggregated+Data+Download+Specification
   */
  @Parameters
  public static Collection<Object[]> data() {
    // TODO: add remaining tests
    return ImmutableList.copyOf(new Object[][] {
        { "6", 123456L, 123458L, "TTC>-", DELETION, "TTC", new VCFMutation(123455L, of("A"), "ATTC", "ATTC>A") },
        { "6", 1L, 1L, "N>-", DELETION, "N", new VCFMutation(1L, of("N"), "NN", "NN>N") },
        { "9", 100000L, 1000L, "->A", INSERTION, "-", new VCFMutation(99999L, of("CA"), "C", "C>CA") },
        { "9", 1L, 1L, "N>A", INSERTION, "N", new VCFMutation(1L, of("AN"), "N", "N>AN") },
        { "6", 123456L, 123456L, "C>T", SINGLE_BASE_SUBSTITUTION, "T", new VCFMutation(123456L, of("C"), "T", "C>T") },
        { "6", 123456L, 123456L, "A>T", SINGLE_BASE_SUBSTITUTION, "A", new VCFMutation(123456L, of("T"), "A", "A>T") },
    });
  }

  @Test
  public void testConvert() {
    val actual = converter.convert(chromosome, start, end, mutation, type, reference);

    assertThat(actual).isEqualTo(expected);
  }

}
