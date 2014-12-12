/* Insert specimen id */
INSERT INTO specimen_ids 
	(specimen_id, project_id, creation_release) 
VALUES 
	(:specimenId, :projectId, :creationRelease)
