/* Insert donor id */
INSERT INTO donor_ids 
	(donor_id, project_id, creation_release) 
VALUES 
	(:donorId, :projectId, :creationRelease)
