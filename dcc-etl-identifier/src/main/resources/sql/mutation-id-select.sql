/* Find mutation id */
SELECT
	m.id 
FROM
	mutation_ids m
WHERE 
	m.chromosome       = :chromosome      AND 
	m.chromosome_start = :chromosomeStart AND
	m.chromosome_end   = :chromosomeEnd   AND
	m.mutation_type    = :mutationType    AND
	m.mutation         = :mutation        AND
	m.assembly_version    = :assemblyVersion
