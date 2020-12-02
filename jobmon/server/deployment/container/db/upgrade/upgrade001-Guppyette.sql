use `docker`;

-- remove partitioning of workflow.
ALTER TABLE workflow
REMOVE PARTITIONING;

ALTER TABLE workflow
DROP PRIMARY KEY,
DROP COLUMN partition_date,
ADD PRIMARY KEY(id);

ALTER TABLE workflow
PARTITION BY range(`id`)(
    PARTITION future VALUES LESS THAN (MAXVALUE)
);


-- remove partitioning of workflow_run.
ALTER TABLE workflow_run
REMOVE PARTITIONING;

ALTER TABLE workflow_run
DROP PRIMARY KEY,
DROP COLUMN partition_date,
ADD PRIMARY KEY(id);

ALTER TABLE workflow_run
PARTITION BY range(`id`)(
    PARTITION future VALUES LESS THAN (MAXVALUE)
);


-- remove partitioning of executor_parameter_set
ALTER TABLE executor_parameter_set
REMOVE PARTITIONING;

ALTER TABLE executor_parameter_set
DROP PRIMARY KEY,
DROP COLUMN partition_date,
ADD PRIMARY KEY(id);

ALTER TABLE executor_parameter_set
PARTITION BY range(`id`)(
    PARTITION future VALUES LESS THAN (MAXVALUE)
);


-- remove partitioning of task
ALTER TABLE task
REMOVE PARTITIONING;

ALTER TABLE task
DROP PRIMARY KEY,
DROP COLUMN partition_date,
ADD PRIMARY KEY(id);

ALTER TABLE task
PARTITION BY range(`id`)(
    PARTITION future VALUES LESS THAN (MAXVALUE)
);


-- remove partitioning of task_instance
ALTER TABLE task_instance
REMOVE PARTITIONING;

ALTER TABLE task_instance
DROP PRIMARY KEY,
DROP COLUMN partition_date,
ADD PRIMARY KEY(id);

ALTER TABLE task_instance
PARTITION BY range(`id`)(
    PARTITION future VALUES LESS THAN (MAXVALUE)
);

-- remove partitioning of task_instance_error_log
ALTER TABLE task_instance_error_log
REMOVE PARTITIONING;

ALTER TABLE task_instance_error_log
DROP PRIMARY KEY,
DROP COLUMN partition_date,
ADD PRIMARY KEY(id);

ALTER TABLE task_instance_error_log
PARTITION BY range(`id`)(
    PARTITION future VALUES LESS THAN (MAXVALUE)
);


-- remove partition of task_attribute
ALTER TABLE task_attribute
REMOVE PARTITIONING;

ALTER TABLE task_attribute
DROP PRIMARY KEY,
DROP COLUMN partition_date,
DROP COLUMN id,
DROP KEY task_id,
DROP KEY attribute_type_id,
CHANGE COLUMN attribute_type_id task_attribute_type_id INT DEFAULT NULL,
ADD PRIMARY KEY (task_id, task_attribute_type_id);

-- remove column from workflow_attribute
ALTER TABLE workflow_attribute
DROP COLUMN partition_date;

-- remove partition of task_attribute_type
ALTER TABLE task_attribute_type
REMOVE PARTITIONING;

ALTER TABLE task_attribute_type
DROP PRIMARY KEY,
DROP COLUMN partition_date,
ADD PRIMARY KEY(id);

ALTER TABLE task_attribute_type
ADD CONSTRAINT unique_names UNIQUE(name);

-- new value for `task_instance_status`

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
