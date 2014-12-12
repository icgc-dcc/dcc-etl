ALTER TABLE mutation_ids RENAME COLUMN mutation_type TO mutation_type_bk;
ALTER TABLE mutation_ids RENAME COLUMN mutation TO mutation_bk;
ALTER TABLE mutation_ids RENAME COLUMN mutation_type_bk TO mutation;
ALTER TABLE mutation_ids RENAME COLUMN mutation_bk TO mutation_type;
