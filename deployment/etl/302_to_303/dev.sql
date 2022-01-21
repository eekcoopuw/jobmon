-- Due to edge volume,
-- Set timeout setting first: Edit -> SQL Editor -> DBMS connection read timeout interval + DBMS connection timeout interval 6000
USE docker;

ALTER TABLE cluster CHANGE connection_string connection_parameters VARCHAR(2500);

UPDATE cluster
SET connection_parameters = '{"slurm_rest_host": "https://api-stage.cluster.ihme.washington.edu", "slurmtool_token_host": "https://slurmtool-stage.ihme.washington.edu/api/v1/token/"}'
WHERE id IN (
	SELECT c.id
    FROM cluster c
		INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
        WHERE ct.name = 'slurm');

ALTER TABLE edge
	CHANGE upstream_node_ids upstream_node_ids LONGTEXT,
	CHANGE downstream_node_ids downstream_node_ids LONGTEXT;
