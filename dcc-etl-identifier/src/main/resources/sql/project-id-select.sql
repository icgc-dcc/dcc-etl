/* Find project id */
SELECT
	p.id 
FROM 
	project_ids p 
WHERE 
	p.project_id = :projectId