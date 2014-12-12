/* Find donor id */
SELECT
	d.id 
FROM
	donor_ids d
WHERE 
	d.donor_id   = :donorId AND 
	d.project_id = :projectId