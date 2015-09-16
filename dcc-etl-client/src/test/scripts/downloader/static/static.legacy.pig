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

