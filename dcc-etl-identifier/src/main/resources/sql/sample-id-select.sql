/* Find sample id */
SELECT
	s.id 
FROM
	sample_ids s
WHERE 
	s.sample_id  = :sampleId AND 
	s.project_id = :projectId