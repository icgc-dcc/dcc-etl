/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.db.importer.cghub.util;

import static lombok.AccessLevel.PRIVATE;

import java.util.Map;
import java.util.Set;

import lombok.NoArgsConstructor;

import com.google.common.collect.ImmutableMap;

@NoArgsConstructor(access = PRIVATE)
public class CGHubProjects {

  private static final Map<String, String> DISEASE_CODE_PROJECT_IDS = ImmutableMap.<String, String> builder()
      .put("BLCA", "BLCA-US")
      .put("BRCA", "BRCA-US")
      .put("CESC", "CESC-US")
      .put("CNTL", "Control")
      .put("COAD", "COAD-US")
      .put("DLBC", "unknown.1")
      .put("ESCA", "unknown.2")
      .put("GBM", "GBM-US")
      .put("HNSC", "HNSC-US")
      .put("KICH", "unknown.3")
      .put("KIRC", "KIRC-US")
      .put("KIRP", "KIRP-US")
      .put("LAML", "LAML-US")
      .put("LCLL", "unknown.4")
      .put("LGG", "LGG-US")
      .put("LIHC", "LIHC-US")
      .put("LUAD", "LUAD-US")
      .put("LUSC", "LUSC-US")
      .put("MESO", "unknown.5")
      .put("MM", "unknown.6")
      .put("OV", "OV-US")
      .put("PAAD", "PAAD-US")
      .put("PRAD", "PRAD-US")
      .put("READ", "READ-US")
      .put("SARC", "SARC-US")
      .put("SKCM", "SKCM-US")
      .put("STAD", "STAD-US")
      .put("THCA", "THCA-US")
      .put("UCEC", "UCEC-US")
      .build();

  public static Set<String> getProjects() {
    return DISEASE_CODE_PROJECT_IDS.keySet();
  }

  public static String getProjectId(String diseaseCode) {
    return DISEASE_CODE_PROJECT_IDS.get(diseaseCode);
  }

}
