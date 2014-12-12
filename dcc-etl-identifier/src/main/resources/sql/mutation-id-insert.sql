/* Insert mutation id */
INSERT INTO mutation_ids
	(chromosome, chromosome_start, chromosome_end, mutation_type, mutation, assembly_version, creation_release) 
VALUES 
	(:chromosome, :chromosomeStart, :chromosomeEnd, :mutationType, :mutation, :assemblyVersion, :creationRelease) 
