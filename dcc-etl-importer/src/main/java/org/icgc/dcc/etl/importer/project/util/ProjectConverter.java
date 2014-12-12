/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.importer.project.util;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.etl.importer.project.util.ProjectFieldCleaner.cleanField;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.val;

import org.icgc.dcc.common.client.api.cgp.CancerGenomeProject;
import org.icgc.dcc.common.client.api.cgp.DataLevelProject;
import org.icgc.dcc.etl.importer.project.model.Project;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@NoArgsConstructor(access = PRIVATE)
public final class ProjectConverter {

  /**
   * Constants.
   */
  private static final String _PROJECT_ID_KEY = "field_dcc_project_code";
  private static final String ALIAS_KEY = "field_dcc_project_alias";
  private static final String PROJECT_NAME_KEY = "field_dcc_project_name";
  private static final String TUMOR_TYPE_KEY = "field_cgp_tumor_type";
  private static final String TUMOR_SUBTYPE_KEY = "field_cgp_tumor_subtype";
  private static final String PRIMARY_COUNTRIES_KEY = "field_cgp_cou_pri";
  private static final String PARTNER_COUNTRIES_KEY = "field_cgp_cou_part";
  private static final String PUBMED_IDS_KEY = "field_pmid";

  private static final List<String> SEPARATORS = ImmutableList.of(",", " ", "\\n");
  private static final Map<String, String> PRIMARY_SITE_MAPPING = ImmutableMap.<String, String> builder()
      .put("Bladder and Urinary Tract", "Bladder")
      .put("Bone", "Bone")
      .put("Breast", "Breast")
      .put("Central Nervous System", "Brain")
      .put("Cervix", "Cervix")
      .put("Colorectal", "Colorectal")
      .put("Esophagus", "Esophagus")
      .put("Gall Bladder and Biliary System", "Gall Bladder")
      .put("Head, Neck and Nasopharynx", "Head and neck")
      .put("Hematopoietic and Lymphoid Tissues", "Blood")
      .put("Kidney", "Kidney")
      .put("Liver", "Liver")
      .put("Lung", "Lung")
      .put("Ovary", "Ovary")
      .put("Pancreas", "Pancreas")
      .put("Skin", "Skin")
      .put("Stomach", "Stomach")
      .put("Testis and Prostate", "Prostate")
      .put("Uterus", "Uterus")
      .build();

  public static Iterable<Project> convertCgp(CancerGenomeProject cgp) {
    val cgpDetails = cgp.getDetails();

    val projects = ImmutableList.<Project> builder();
    for (val dlp : cgp.getDlps()) {
      val dlpProject = convertDlp(cgp, cgpDetails, dlp);

      projects.add(dlpProject);
    }

    return projects.build();
  }

  private static Project convertDlp(CancerGenomeProject cgp, Map<String, String> cgpDetails, DataLevelProject dlp) {
    val dlpDetails = dlp.getDetails();

    return Project.builder()
        .__project_id(dlpDetails.get(_PROJECT_ID_KEY))
        .icgc_id(cgp.getNid())
        .alias(dlpDetails.get(ALIAS_KEY))
        .project_name(dlpDetails.get(PROJECT_NAME_KEY))
        .tumour_type(cleanField(cgpDetails.get(TUMOR_TYPE_KEY)))
        .tumour_subtype(cleanField(cgpDetails.get(TUMOR_SUBTYPE_KEY)))
        .primary_site(convertPrimarySite(cgp, dlpDetails.get(_PROJECT_ID_KEY)))
        .primary_countries(convertCountries(cgpDetails.get(PRIMARY_COUNTRIES_KEY)))
        .partner_countries(convertCountries(cgpDetails.get(PARTNER_COUNTRIES_KEY)))
        .pubmed_ids(convertPubmedIds(dlpDetails.get(PUBMED_IDS_KEY)))
        .build();
  }

  private static String convertPrimarySite(CancerGenomeProject cgp, String projectId) {
    if (projectId.equals("RTBL-FR")) {
      return "Eye";
    } else if (projectId.equals("NACA-CN")) {
      return "Nasopharynx";
    } else {
      val organSystem = cgp.getOrganSystem();
      val primarySite = PRIMARY_SITE_MAPPING.get(organSystem);

      checkState(primarySite != null, "Mapping not found for project id '%s' and primary site %s",
          projectId, organSystem);

      return primarySite;
    }
  }

  private static List<String> convertCountries(String countries) {
    if (isNullOrEmpty(countries)) {
      return Collections.emptyList();
    }

    val result = new ImmutableList.Builder<String>();
    for (val countryCode : countries.split("_")) {
      result.add(convertCountryName(countryCode));
    }

    return result.build();
  }

  private static String convertCountryName(String countryCode) {
    // to align with the Project.json
    if (countryCode.toLowerCase().equals("eu")) return "European Union";

    return new Locale("", countryCode).getDisplayCountry();
  }

  private static List<String> convertPubmedIds(String ids) {
    if (isNullOrEmpty(ids)) return Collections.emptyList();
    if (ids.matches("^\\d+$")) return ImmutableList.of(ids);

    return split(ids);
  }

  private static List<String> split(String source) {
    if (source == null) return Collections.emptyList();

    val separator = getSeparator(source);
    val result = new ImmutableList.Builder<String>();
    for (val id : source.split(separator)) {
      result.add(id);
    }

    return result.build();
  }

  private static String getSeparator(String source) {
    for (val separator : SEPARATORS) {
      if (source.contains(separator)) return separator;
    }

    throw new IllegalArgumentException("Invalid separator used: " + source);
  }

}
