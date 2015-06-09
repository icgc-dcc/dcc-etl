/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS"AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.etl.repo.model;

import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterables.tryFind;
import static lombok.AccessLevel.PRIVATE;

import java.util.List;

import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import org.icgc.dcc.etl.repo.model.RepositoryProjects.RepositoryProject.RepositoryProjectBuilder;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

@NoArgsConstructor(access = PRIVATE)
public final class RepositoryProjects {

  /**
   * Constants.
   */
  // @formatter:off
  public static final List<RepositoryProject> PROJECTS = ImmutableList.of(
      project().projectCode("ALL-US") .diseaseCode("ALL") .program("TARGET").country("United States").build(),
      project().projectCode("AML-US") .diseaseCode("AML") .program("TARGET").country("United States").build(),
      project().projectCode("BLCA-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("BLCA-US").diseaseCode("BLCA").program("TCGA")  .country("United States").build(),
      project().projectCode("BOCA-FR").diseaseCode(null)  .program(null)    .country("France").build(),
      project().projectCode("BOCA-UK").diseaseCode(null)  .program(null)    .country("United Kingdom").build(),
      project().projectCode("BRCA-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("BRCA-EU").diseaseCode(null)  .program(null)    .country("European Union/United Kingdom").build(),
      project().projectCode("BRCA-FR").diseaseCode(null)  .program(null)    .country("France").build(),
      project().projectCode("BRCA-KR").diseaseCode(null)  .program(null)    .country("South Korea").build(),
      project().projectCode("BRCA-MX").diseaseCode(null)  .program(null)    .country("Mexico").build(),
      project().projectCode("BRCA-UK").diseaseCode(null)  .program(null)    .country("United Kingdom").build(),
      project().projectCode("BRCA-US").diseaseCode("BRCA").program("TCGA")  .country("United States").build(),
      project().projectCode("BTCA-SG").diseaseCode(null)  .program(null)    .country("Singapore").build(),
      project().projectCode("CESC-US").diseaseCode("CESC").program("TCGA")  .country("United States").build(),
      project().projectCode("CLLE-ES").diseaseCode(null)  .program(null)    .country("Spain").build(),
      project().projectCode("CMDI-UK").diseaseCode(null)  .program(null)    .country("United Kingdom").build(),
      project().projectCode("COAD-US").diseaseCode("COAD").program("TCGA")  .country("United States").build(),
      project().projectCode("COCA-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("DLBC-US").diseaseCode("DLBC").program("TCGA")  .country("United States").build(),
      project().projectCode("EOPC-DE").diseaseCode(null)  .program(null)    .country("Germany").build(),
      project().projectCode("ESAD-UK").diseaseCode(null)  .program(null)    .country("United Kingdom").build(),
      project().projectCode("ESCA-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("GACA-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("GBM-CN") .diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("GBM-US") .diseaseCode("GBM") .program("TCGA")  .country("United States").build(),
      project().projectCode("HNCA-MX").diseaseCode(null)  .program(null)    .country("Mexico").build(),
      project().projectCode("HNSC-US").diseaseCode("HNSC").program("TCGA")  .country("United States").build(),
      project().projectCode("KICH-US").diseaseCode("KICH").program("TCGA")  .country("United States").build(),
      project().projectCode("KIRC-US").diseaseCode("KIRC").program("TCGA")  .country("United States").build(),
      project().projectCode("KIRP-US").diseaseCode("KIRP").program("TCGA")  .country("United States").build(),
      project().projectCode("LAML-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("LAML-KR").diseaseCode(null)  .program(null)    .country("South Korea").build(),
      project().projectCode("LAML-US").diseaseCode("LAML").program("TCGA")  .country("United States").build(),
      project().projectCode("LGG-US") .diseaseCode("LGG") .program("TCGA")  .country("United States").build(),
      project().projectCode("LIAD-FR").diseaseCode(null)  .program(null)    .country("France").build(),
      project().projectCode("LICA-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("LICA-FR").diseaseCode(null)  .program(null)    .country("France").build(),
      project().projectCode("LIHC-US").diseaseCode("LIHC").program("TCGA")  .country("United States").build(),
      project().projectCode("LIHM-FR").diseaseCode(null)  .program(null)    .country("France").build(),
      project().projectCode("LINC-JP").diseaseCode(null).  program(null)    .country("Japan").build(),
      project().projectCode("LIRI-JP").diseaseCode(null)  .program(null)    .country("Japan").build(),
      project().projectCode("LMS-FR") .diseaseCode(null)  .program(null)    .country("France").build(),
      project().projectCode("LUAD-US").diseaseCode("LUAD").program("TCGA")  .country("United States").build(),
      project().projectCode("LUSC-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("LUSC-KR").diseaseCode(null)  .program(null)    .country("South Korea").build(),
      project().projectCode("LUSC-US").diseaseCode("LUSC").program("TCGA")  .country("United States").build(),
      project().projectCode("MALY-DE").diseaseCode(null)  .program(null)    .country("Germany").build(),
      project().projectCode("MELA-AU").diseaseCode(null)  .program(null)    .country("Australia").build(),
      project().projectCode("NACA-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("NBL-US") .diseaseCode("NBL") .program("TARGET").country("United States").build(),
      project().projectCode("NHLY-MX").diseaseCode(null)  .program(null)    .country("Mexico").build(),
      project().projectCode("ORCA-IN").diseaseCode(null)  .program(null)    .country("India").build(),
      project().projectCode("OS-US")  .diseaseCode("OS")  .program("TARGET").country("United States").build(),
      project().projectCode("OV-AU")  .diseaseCode(null)  .program(null)    .country("Australia").build(),
      project().projectCode("OV-CN")  .diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("OV-US")  .diseaseCode("OV")  .program("TCGA")  .country("United States").build(),
      project().projectCode("PAAD-US").diseaseCode("PRAD").program("TCGA")  .country("United States").build(),
      project().projectCode("PACA-AU").diseaseCode(null)  .program(null)    .country("Australia").build(),
      project().projectCode("PACA-CA").diseaseCode(null)  .program(null)    .country("Canada").build(),
      project().projectCode("PACA-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("PACA-IT").diseaseCode(null)  .program(null)    .country("India").build(),
      project().projectCode("PAEN-AU").diseaseCode(null)  .program(null)    .country("Australia").build(),
      project().projectCode("PBCA-DE").diseaseCode(null)  .program(null)    .country("Germany").build(),
      project().projectCode("PEME-CA").diseaseCode(null)  .program(null)    .country("Canada").build(),
      project().projectCode("PRAD-CA").diseaseCode(null)  .program(null)    .country("Canada").build(),
      project().projectCode("PRAD-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("PRAD-UK").diseaseCode(null)  .program(null)    .country("United Kingdom").build(),
      project().projectCode("PRAD-US").diseaseCode("PRAD").program("TCGA")  .country("United States").build(),
      project().projectCode("PRCA-FR").diseaseCode(null)  .program(null)    .country("France").build(),
      project().projectCode("READ-US").diseaseCode("READ").program("TCGA")  .country("United States").build(),
      project().projectCode("RECA-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("RECA-EU").diseaseCode(null)  .program(null)    .country("European Union/United Kingdom").build(),
      project().projectCode("RT-US")  .diseaseCode("RT")  .program("TARGET").country("United States").build(),
      project().projectCode("RML-US") .diseaseCode("RML") .program("TARGET").country("United States").build(),
      project().projectCode("RTBL-FR").diseaseCode(null)  .program(null)    .country("France").build(),
      project().projectCode("SARC-US").diseaseCode("SARC").program("TCGA")  .country("United States").build(),
      project().projectCode("SKCA-BR").diseaseCode(null)  .program(null)    .country("Brazil").build(),
      project().projectCode("SKCM-US").diseaseCode("SKCM").program("TCGA")  .country("United States").build(),
      project().projectCode("STAD-US").diseaseCode("STAD").program("TCGA")  .country("United States").build(),
      project().projectCode("THCA-CN").diseaseCode(null)  .program(null)    .country("China").build(),
      project().projectCode("THCA-SA").diseaseCode(null)  .program(null)    .country("Saudi Arabia").build(),
      project().projectCode("THCA-US").diseaseCode("THCA").program("TCGA")  .country("United States").build(),
      project().projectCode("UCEC-US").diseaseCode("UCEC").program("TCGA")  .country("United States").build(),
      project().projectCode("WT-US")  .diseaseCode("WT")  .program("TARGET").country("United States").build()     
      );
  // @formatter:on

  @NonNull
  public static Iterable<RepositoryProject> getProjects() {
    return PROJECTS;
  }

  @NonNull
  public static Optional<RepositoryProject> getProjectCodeProject(String projectCode) {
    return tryFind(getProjects(), project -> projectCode.equals(project.getProjectCode()));
  }

  @NonNull
  public static Optional<RepositoryProject> getDiseaseCodeProject(String diseaseCode) {
    return tryFind(getProjects(), project -> diseaseCode.equals(project.getDiseaseCode()));
  }

  public static Iterable<RepositoryProject> getTCGAProjects() {
    return filter(getProjects(), project -> "TCGA".equals(project.getProgram()));
  }

  public static Iterable<RepositoryProject> getTARGETProjects() {
    return filter(getProjects(), project -> "TARGET".equals(project.getProgram()));
  }

  public static Iterable<String> getProjectDiseaseCodes() {
    return filter(transform(getProjects(), project -> project.getDiseaseCode()), notNull());
  }

  @Value
  @Builder
  public static class RepositoryProject {

    String projectCode;
    String diseaseCode;
    String country;
    String program;

  }

  private static RepositoryProjectBuilder project() {
    return RepositoryProject.builder();
  }

}
