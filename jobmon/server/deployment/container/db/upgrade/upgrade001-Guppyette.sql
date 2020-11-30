use `docker`;

-- remove partition of task_attribute

ALTER TABLE task_attribute_type
REMOVE PARTITIONING;

ALTER TABLE task_attribute_type
DROP PRIMARY KEY,
DROP COLUMN partition_date,
ADD PRIMARY KEY(id);

ALTER TABLE task_attribute_type
ADD CONSTRAINT unique_names UNIQUE(name);


-- Table structure for table `task_instance_status`


INSERT INTO `task_instance_status` VALUES ('F','ERROR_FATAL');

/*
-- name changes
ALTER TABLE task_attribute
CHANGE COLUMN `attribute_type` attribute_type_id int;
ALTER TABLE edge
CHANGE COLUMN `upstream_nodes` upstream_node_ids text
ALTER TABLE edge
CHANGE COLUMN `downstream_node_ids` downstream_node_ids text
RENAME TABLE command_template_arg_type_mapping
TO template_arg_map
*/


