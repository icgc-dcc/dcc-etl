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
package org.icgc.dcc.etl.db.importer.pcawg.util;

import static lombok.AccessLevel.PRIVATE;

import java.util.List;

import lombok.NoArgsConstructor;

import com.google.common.collect.ImmutableList;

@NoArgsConstructor(access = PRIVATE)
public class PCAWGArchives {

  /**
   * URLs.
   */
  public static final String PCAWG_ARCHIVE_BASE_URL = "http://pancancer.info/gnos_metadata";

  /**
   * Field names.
   */
  public static final String PCAWG_SUBMITTER_DONOR_ID = "submitter_donor_id";
  public static final String PCAWG_DCC_PROJECT_CODE = "dcc_project_code";
  public static final String PCAWG_SUBMITTER_SPECIMEN_ID = "submitter_specimen_id";
  public static final String PCAWG_SUBMITTER_SAMPLE_ID = "submitter_sample_id";
  public static final String PCAWG_FILES_FIELD = "files";

  /**
   * Field values.
   */
  public static final List<String> PCAWG_SPECIMEN_CLASSES = ImmutableList.of(
      "normal_specimen", "normal_specimens", "tumor_specimen", "tumor_specimens");
  public static final List<String> PCAWG_WORKFLOW_TYPES = ImmutableList.of(
      "star", "tophat", "bwa_alignment", "sanger_variant_calling");
  public static final List<String> PCAWG_LIBRARY_STRATEGY_NAMES = ImmutableList.of(
      "rna_seq", "wgs");

}
