/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.repo.pcawg.core;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.List;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

import org.icgc.dcc.etl.repo.model.RepositoryFile.RepositoryFileDataType;

import com.google.common.collect.ImmutableList;

@UtilityClass
public class PCAWGFileDataTypeResolver {

  @NonNull
  public static List<RepositoryFileDataType> resolveFileDataTypes(String analysisType, String fileName) {
    if (isRNASeq(analysisType)) {
      return singletonList(new RepositoryFileDataType()
          .setDataType("RNA-Seq")
          .setDataFormat("BAM")
          .setExperimentalStrategy("RNA-Seq"));
    } else if (isDNASeq(analysisType)) {
      return singletonList(new RepositoryFileDataType()
          .setDataType("DNA-Seq")
          .setDataFormat("BAM")
          .setExperimentalStrategy("WGS"));
    } else if (isSangerVariantCalling(analysisType)) {
      return resolveSangerVariantCallingDataTypes(fileName)
          .stream()
          .map(dataType ->
              new RepositoryFileDataType()
                  .setDataType(dataType)
                  .setDataFormat("Other".equals(dataType) ? "Other" : "VCF")
                  .setExperimentalStrategy("WGS")
          )
          .collect(toImmutableList());
    }

    return emptyList();
  }

  private static boolean isRNASeq(String analysisType) {
    return analysisType.matches("rna_seq\\..*\\.(star|tophat)");
  }

  private static boolean isDNASeq(String analysisType) {
    return analysisType.matches("wgs\\..*\\.bwa_alignment");
  }

  private static boolean isSangerVariantCalling(String analysisType) {
    return analysisType.matches("wgs\\.tumor_specimens\\.sanger_variant_calling");
  }

  private static List<String> resolveSangerVariantCallingDataTypes(String fileName) {
    if (fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
      return ImmutableList.of("Simple Somatic Mutations");
    } else if (fileName.endsWith(".somatic.cnv.vcf.gz")) {
      return ImmutableList.of("Copy Number Somatic Mutations");
    } else if (fileName.endsWith(".somatic.sv.vcf.gz")) {
      return ImmutableList.of("Structural Somatic Mutations");
    } else if (fileName.endsWith(".somatic.indel.vcf.gz")) {
      return ImmutableList.of("Simple Somatic Mutations");
    } else if (fileName.endsWith(".somatic.snv_mnv.tar.gz")) {
      return ImmutableList.of("Simple Somatic Mutations", "Simple Germline Variants");
    } else if (fileName.endsWith(".somatic.cnv.tar.gz")) {
      return ImmutableList.of("Copy Number Somatic Mutations", "Copy Number Germline Variants");
    } else if (fileName.endsWith(".somatic.sv.tar.gz")) {
      return ImmutableList.of("Structural Somatic Mutations", "Structural Germline Variants");
    } else if (fileName.endsWith(".somatic.indel.tar.gz")) {
      return ImmutableList.of("Simple Somatic Mutations", "Simple Germline Variants");
    } else if (fileName.endsWith(".somatic.imputeCounts.tar.gz")) {
      return ImmutableList.of("Other");
    } else if (fileName.endsWith(".somatic.binnedReadCounts.tar.gz")) {
      return ImmutableList.of("Other");
    } else if (fileName.endsWith(".somatic.genotype.tar.gz")) {
      return ImmutableList.of("Other");
    } else if (fileName.endsWith(".somatic.verifyBamId.tar.gz")) {
      return ImmutableList.of("Other");
    } else {
      return emptyList();
    }
  }

}
