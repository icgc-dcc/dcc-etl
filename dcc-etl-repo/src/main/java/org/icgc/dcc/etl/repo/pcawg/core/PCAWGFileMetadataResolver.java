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

import lombok.Data;
import lombok.NonNull;
import lombok.val;
import lombok.experimental.Accessors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class PCAWGFileMetadataResolver {

  @NonNull
  public static PCAWGFileMetadata resolveMetadata(String analysisType, String fileName) {
    val metadata = new PCAWGFileMetadata()
        .setAccess("controlled");

    if (analysisType.matches("rna_seq\\..*\\.(star|tophat)")) {
      return metadata
          .setDataType("RNA-Seq")
          .setDataFormat("BAM")
          .setExperimentalStrategy("RNA-Seq");
    } else if (analysisType.matches("wgs\\..*\\.bwa_alignment")) {
      return metadata
          .setDataType("DNA-Seq")
          .setDataFormat("BAM")
          .setExperimentalStrategy("RNA-Seq");
    } else if (analysisType.matches("wgs\\.tumor_specimens\\.sanger_variant_calling")) {
      return metadata
          .setDataType(resolveSangerVCFDataType(fileName, fileName))
          .setDataFormat("VCF")
          .setExperimentalStrategy("WGS");
    }

    return metadata;
  }

  private static String resolveSangerVCFDataType(String fileName, String dataType) {
    if (fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
      dataType = "Somatic SNV MNV";
    } else if (fileName.endsWith(".somatic.cnv.vcf.gz")) {
      dataType = "Somatic CNV";
    } else if (fileName.endsWith(".somatic.sv.vcf.gz")) {
      dataType = "Somatic SV";
    } else if (fileName.endsWith(".somatic.indel.vcf.gz")) {
      dataType = "Somatic Indel";
    } else if (fileName.endsWith(".somatic.snv_mnv.tar.gz")) {
      dataType = "Somatic SNV MNV";
    } else if (fileName.endsWith(".somatic.sv.tar.gz")) {
      dataType = "Somatic SV";
    } else if (fileName.endsWith(".somatic.indel.tar.gz")) {
      dataType = "Somatic Indel";
    } else if (fileName.endsWith(".somatic.imputeCounts.tar.gz")) {
      dataType = "Somatic Impute Counts";
    } else if (fileName.endsWith(".somatic.binnedReadCounts.tar.gz")) {
      dataType = "Somatic Binned Read Counts";
    } else if (fileName.endsWith(".somatic.genotype.tar.gz")) {
      dataType = "Somatic Genotype";
    } else if (fileName.endsWith(".somatic.verifyBamId.tar.gz")) {
      dataType = "Somatic Verify BAM Id";
    }
    return dataType;
  }

  @Data
  @Accessors(chain = true)
  public static class PCAWGFileMetadata {

    String dataType;
    String dataFormat;
    String experimentalStrategy;
    String access;

  }

}
