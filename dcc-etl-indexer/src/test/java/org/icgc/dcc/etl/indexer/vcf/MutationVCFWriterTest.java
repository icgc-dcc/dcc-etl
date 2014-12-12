package org.icgc.dcc.etl.indexer.vcf;

import static com.google.common.collect.ImmutableList.of;
import static org.icgc.dcc.etl.indexer.vcf.model.MutationType.SINGLE_BASE_SUBSTITUTION;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import lombok.val;

import org.icgc.dcc.etl.indexer.vcf.model.Consequence;
import org.icgc.dcc.etl.indexer.vcf.model.Feature;
import org.icgc.dcc.etl.indexer.vcf.model.Mutation;
import org.icgc.dcc.etl.indexer.vcf.model.Occurrence;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MutationVCFWriterTest {

  /**
   * Input.
   */
  private static final String RELEASE_NAME = "ICGCr16";
  private static final File REFERENCE_GENOME = new File("/tmp/GRCh37.fasta");

  /**
   * Output.
   */
  private static final File OUTPUT_FILE = new File("target/ssm.release16.controlled.vcf");

  @Test
  public void testWrite() throws IOException {
    val writer = new MutationVCFWriter(RELEASE_NAME, REFERENCE_GENOME, new FileOutputStream(OUTPUT_FILE), true, 100);
    try {
      writer.writeHeader();
      writer.writeFeature(
          Feature.builder()
              .id("MU1")
              .chromosome("6")
              .chromosomeStart(1)
              .chromosomeEnd(2)
              .altAllele("TT")
              .mutation(Mutation.builder()
                  .mutationType(SINGLE_BASE_SUBSTITUTION)
                  .mutation("A>C")
                  .affectedDonors(1)
                  .testedDonors(2)
                  .projectCount(3)
                  .occurrences(of(
                      new Occurrence()
                          .projectCode("COAD-US ")
                          .affectedDonors(15)
                          .testedDonors(261)
                          .frequency("0.06"),
                      new Occurrence()
                          .projectCode("READ-US")
                          .affectedDonors(12)
                          .testedDonors(109)
                          .frequency("0.11"),
                      new Occurrence()
                          .projectCode("OV-US")
                          .affectedDonors(1)
                          .testedDonors(278)
                          .frequency("0.0478")
                      ))
                  .consequences(of(
                      new Consequence()
                          .geneSymbol("TBP")
                          .geneAffected("ENSG00000112592")
                          .geneStrand("+")
                          .transcriptName("TBP-001")
                          .transcriptAffected("ENST00000230354")
                          .proteinAffected("ENSP00000230354")
                          .consequenceType("inframe_deletion")
                          .cdsMutation("c.214CAA>-")
                          .aaMutation("Q72-"),
                      new Consequence()
                          .geneSymbol("TBP")
                          .geneAffected("ENSG00000112592")
                          .geneStrand("+")
                          .transcriptName("TBP-202")
                          .transcriptAffected("ENST00000540980")
                          .proteinAffected("ENSP00000442132")
                          .consequenceType("inframe_deletion")
                          .cdsMutation("c.154CAA>-")
                          .aaMutation("Q52-")
                      )
                  )
                  .build())
              .build());
    } finally {
      writer.close();
    }
  }

}
