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

-- TODO: number of reduce tasks

-- pig -param run_name=load-prod-06e-40-23 -param run_number=6 -param project_id=LICA-FR normalizer.pig

ssm_original = LOAD '/icgc/testing/$run_name-$run_number/$project_id.ssm/part-r-00000'; -- TODO: can use LOAD ... AS ... (saves one line)
ssm_original = FOREACH ssm_original GENERATE $8 AS assembly_version, $10 AS chromosome, $12 AS chromosome_end, $11 AS chromosome_start, $13 AS chromosome_strand, $15 AS control_genotype, $18 AS gene_affected, $1 AS icgc_donor_id, $0 AS icgc_mutation_id, $17 AS mutation, $9 AS mutation_type, $14 AS reference_genome_allele, $16 AS tumour_genotype;
ssm_original = DISTINCT ssm_original;
ssm_original = ORDER ssm_original BY icgc_donor_id, icgc_mutation_id;

ssm_static = LOAD '/icgc/download/static/release_14/$project_id/simple_somatic_mutation.$project_id.tsv.gz';
ssm_static = FOREACH ssm_static GENERATE $12 AS assembly_version, $8 AS chromosome, $10 AS chromosome_end, $9 AS chromosome_start, $11 AS chromosome_strand, $16 AS control_genotype, $32 AS gene_affected, $1 AS icgc_donor_id, $0 AS icgc_mutation_id, $14 AS mutation, $13 AS mutation_type, $15 AS reference_genome_allele, $17 AS tumour_genotype;
ssm_static = FILTER ssm_static BY icgc_mutation_id != 'icgc_mutation_id';
ssm_static = DISTINCT ssm_static;
ssm_static = ORDER ssm_static BY icgc_donor_id, icgc_mutation_id;

STORE ssm_original INTO '/icgc/testing/diff/$project_id.original';
STORE ssm_static INTO '/icgc/testing/diff/$project_id.static';

