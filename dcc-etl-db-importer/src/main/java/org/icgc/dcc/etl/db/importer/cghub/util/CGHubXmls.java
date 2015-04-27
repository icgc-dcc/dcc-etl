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

import java.util.Set;

import lombok.NoArgsConstructor;

import com.google.common.collect.ImmutableSet;

@NoArgsConstructor(access = PRIVATE)
public class CGHubXmls {

  /**
   * The CGHub metadata XML element name for each result record.
   */
  public static final String RESULT_ELEMENT_NAME = "Result";

  /**
   * The CGHub metadata XML element name for the project id.
   */
  public static final String PROJECT_ELEMENT_NAME = "disease_abbr";

  public static final Set<String> XPATHS = ImmutableSet.of(
      "/ResultSet/Result/analysis_id",
      "/ResultSet/Result/state",
      // "/ResultSet/Result/last_modified",
      // "/ResultSet/Result/published_date",
      "/ResultSet/Result/center_name",
      "/ResultSet/Result/files/file/filename",
      "/ResultSet/Result/files/file/filesize",
      "/ResultSet/Result/legacy_sample_id",
      "/ResultSet/Result/disease_abbr",
      "/ResultSet/Result/tss_id",
      "/ResultSet/Result/analysis_id",
      "/ResultSet/Result/participant_id",
      "/ResultSet/Result/sample_id",
      "/ResultSet/Result/aliquot_id",
      "/ResultSet/Result/analyte_code",
      "/ResultSet/Result/sample_type",
      "/ResultSet/Result/library_strategy",
      "/ResultSet/Result/platform",
      "/ResultSet/Result/refassem_short_name",
      "/ResultSet/Result/analysis_data_uri");

}
