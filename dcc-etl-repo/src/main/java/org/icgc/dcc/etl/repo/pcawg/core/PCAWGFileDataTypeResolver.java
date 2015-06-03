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

import lombok.NonNull;
import lombok.val;
import lombok.experimental.UtilityClass;

import org.icgc.dcc.etl.repo.model.RepositoryFile.RepositoryFileDataType;

@UtilityClass
public class PCAWGFileDataTypeResolver {

  @NonNull
  public static RepositoryFileDataType resolveFileDataType(String analysisType, String fileName) {
    if (isRNASeq(analysisType)) {
      return new RepositoryFileDataType()
          .setDataType("RNA-Seq")
          .setDataFormat("BAM")
          .setExperimentalStrategy("RNA-Seq");
    } else if (isDNASeq(analysisType)) {
      return new RepositoryFileDataType()
          .setDataType("DNA-Seq")
          .setDataFormat("BAM")
          .setExperimentalStrategy("WGS");
    } else if (isSangerVariantCalling(analysisType)) {
      val dataType = resolveSangerVariantCallingDataType(fileName);
      return new RepositoryFileDataType()
          .setDataType(dataType)
          .setDataFormat(dataType == null ? null : "VCF")
          .setExperimentalStrategy("WGS");
    } else {
      return new RepositoryFileDataType();
    }
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

  private static String resolveSangerVariantCallingDataType(String fileName) {
    if (fileName.endsWith(".somatic.snv_mnv.vcf.gz")) {
      return "SSM";
    } else if (fileName.endsWith(".somatic.cnv.vcf.gz")) {
      return "CNSM";
    } else if (fileName.endsWith(".somatic.sv.vcf.gz")) {
      return "StSM";
    } else if (fileName.endsWith(".somatic.indel.vcf.gz")) {
      return "SSM";
    } else {
      return null;
    }
  }

}
