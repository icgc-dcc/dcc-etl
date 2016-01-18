-- Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
--
-- This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
-- You should have received a copy of the GNU General Public License along with
-- this program. If not, see <http://www.gnu.org/licenses/>.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
-- EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
-- OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
-- SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
-- INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
-- TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
-- OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
-- IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
-- ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

%default OBSERVATION '<observation_dir>';
DEFINE ExtractId org.icgc.dcc.etl.exporter.pig.udf.ExtractId();
%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader'

-- load donor
donor = LOAD '$OBSERVATION' USING $JSON_LOADER('-nestedLoad') as document:map[];
specimen = FOREACH donor GENERATE document#'_donor_id' as icgc_donor_id,
                               (bag{tuple(map[])}) document#'specimen' as specimens;

flat_specimen = FOREACH specimen GENERATE icgc_donor_id,
                                      FLATTEN(specimens) as specimen;

flat_sample = FOREACH flat_specimen GENERATE icgc_donor_id,
                   FLATTEN((bag{tuple(map[])}) specimen#'sample') as s;

flat_study = FOREACH flat_sample GENERATE icgc_donor_id,
                      s#'study' as study;

filter_study = FILTER flat_study by study is not null;
unique_study = DISTINCT filter_study;
study_field  = FOREACH (GROUP unique_study BY icgc_donor_id) GENERATE FLATTEN(unique_study);

projected_donor = FOREACH donor GENERATE ExtractId(document#'_donor_id') as donor_id:int,
                                         document#'_donor_id' as icgc_donor_id,
                                         document#'_project_id' as project_code,
                                         document#'donor_id' as submitted_donor_id,
                                         document#'donor_sex' as donor_sex,
                                         document#'donor_vital_status' as donor_vital_status,
                                         document#'disease_status_last_followup' as disease_status_last_followup,
                                         document#'donor_relapse_type' as donor_relapse_type,
                                         document#'donor_age_at_diagnosis' as donor_age_at_diagnosis,
                                         document#'donor_age_at_enrollment' as donor_age_at_enrollment,
                                         document#'donor_age_at_last_followup' as donor_age_at_last_followup,
                                         document#'donor_relapse_interval' as donor_relapse_interval,
                                         document#'donor_diagnosis_icd10' as donor_diagnosis_icd10,
                                         document#'donor_tumour_staging_system_at_diagnosis' as donor_tumour_staging_system_at_diagnosis,
                                         document#'donor_tumour_stage_at_diagnosis' as donor_tumour_stage_at_diagnosis,
                                         document#'donor_tumour_stage_at_diagnosis_supplemental' as donor_tumour_stage_at_diagnosis_supplemental,
                                         document#'donor_survival_time' as donor_survival_time,
                                         document#'donor_interval_of_last_followup' as donor_interval_of_last_followup,
                                         document#'prior_malignancy' as prior_malignancy,
                                         document#'cancer_type_prior_malignancy' as cancer_type_prior_malignancy,
                                         document#'cancer_history_first_degree_relative' as cancer_history_first_degree_relative;

donor_with_study = JOIN projected_donor by icgc_donor_id LEFT OUTER, study_field BY icgc_donor_id;

selected_donor = FOREACH donor_with_study GENERATE projected_donor::donor_id as donor_id,
                                                   projected_donor::icgc_donor_id as icgc_donor_id,
                                                   projected_donor::project_code as project_code,
                                                   study_field::unique_study::study as study_donor_involved_in,
                                                   projected_donor::submitted_donor_id as submitted_donor_id,
                                                   projected_donor::donor_sex as donor_sex,
                                                   projected_donor::donor_vital_status as donor_vital_status,
                                                   projected_donor::disease_status_last_followup as disease_status_last_followup,
                                                   projected_donor::donor_relapse_type as donor_relapse_type,
                                                   projected_donor::donor_age_at_diagnosis as donor_age_at_diagnosis,
                                                   projected_donor::donor_age_at_enrollment as donor_age_at_enrollment,
                                                   projected_donor::donor_age_at_last_followup as donor_age_at_last_followup,
                                                   projected_donor::donor_relapse_interval as donor_relapse_interval,
                                                   projected_donor::donor_diagnosis_icd10 as donor_diagnosis_icd10,
                                                   projected_donor::donor_tumour_staging_system_at_diagnosis as donor_tumour_staging_system_at_diagnosis,
                                                   projected_donor::donor_tumour_stage_at_diagnosis as donor_tumour_stage_at_diagnosis,
                                                   projected_donor::donor_tumour_stage_at_diagnosis_supplemental as donor_tumour_stage_at_diagnosis_supplemental,
                                                   projected_donor::donor_survival_time as donor_survival_time,
                                                   projected_donor::donor_interval_of_last_followup as donor_interval_of_last_followup,
                                                   projected_donor::prior_malignancy as prior_malignancy,
                                                   projected_donor::cancer_type_prior_malignancy as cancer_type_prior_malignancy,
                                                   projected_donor::cancer_history_first_degree_relative as cancer_history_first_degree_relative;
