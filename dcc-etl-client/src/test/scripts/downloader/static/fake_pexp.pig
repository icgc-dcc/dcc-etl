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

-- pexp:
--
-- pexp_m	[analysis_id, analyzed_sample_id, db_xref, experimental_protocol, note, platform, raw_data_accession, raw_data_repository, uri]
-- pexp_p	[analysis_id, analyzed_sample_id, antibody_id, db_xref, gene_build_version, gene_name, gene_stable_id, normalized_expression_level, note, uri, verification_platform, verification_status]
--
-- pexp_m -> sample [fontsize=8, label="analyzed_sample_id"]
-- pexp_p -> pexp_m [fontsize=8, label="analysis_id,analyzed_sample_id\n(surjective)"]
--
-- 	0	icgc_donor_id
-- 	1	project_code
-- 	2	icgc_specimen_id
-- 	3	icgc_sample_id
-- 	4	submitted_sample_id
-- 	5	analysis_id
-- 	6	analyzed_sample_id
-- 	7	antibody_id
-- 	8	gene_name
-- 	9	gene_stable_id
-- 	10	gene_build_version
-- 	11	normalized_expression_level
-- 	12	verification_status
-- 	13	verification_platform
-- 	14	analysis_id
-- 	15	analyzed_sample_id
-- 	16	platform
-- 	17	experimental_protocol
-- 	18	raw_data_repository
-- 	19	raw_data_accession
--
REGISTER 'udfs.py' using jython as udfs;
DEFINE CLEAR udfs.clear;
set default_parallel $default_parallel
pexp_m = LOAD '$m_file' AS ($m_fields);
pexp_p = LOAD '$p_file' AS ($p_fields);
pexp_m = FILTER pexp_m BY analysis_id != 'analysis_id';
pexp_p = FILTER pexp_p BY analysis_id != 'analysis_id';
pexp_m = FOREACH pexp_m GENERATE $m_normal;
pexp_p = FOREACH pexp_p GENERATE $p_normal;
run -param etl_run_name=$etl_run_name -param project_id=$project_id -param donor_file=$donor_file -param specimen_file=$specimen_file -param sample_file=$sample_file clinical.pig
pexp_m = JOIN
    pexp_m BY analyzed_sample_id,
    clinical BY analyzed_sample_id
  USING 'replicated';
pexp_mXpXs = JOIN
    pexp_p BY (analysis_id, analyzed_sample_id),
    pexp_m BY (analysis_id, pexp_m::analyzed_sample_id)
  USING 'replicated';
pexp =
  FOREACH pexp_mXpXs
  GENERATE
    icgc_donor_id,
    project_code,
    pexp_m::analyzed_sample_id AS submitted_sample_id,
    pexp_m::analysis_id,
    pexp_p::gene_stable_id;
pexp = DISTINCT pexp;
pexp = ORDER pexp BY *;
STORE pexp INTO '$output_dir' USING PigStorage('\t','-schema');

