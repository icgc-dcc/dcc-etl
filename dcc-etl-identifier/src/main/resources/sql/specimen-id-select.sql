/* Find specimen id */
SELECT
	s.id 
FROM
	specimen_ids s
WHERE 
	s.specimen_id   = :specimenId AND 
	s.project_id = :projectId
