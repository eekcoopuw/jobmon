use `docker`;

ALTER TABLE task_attribute_type
REMOVE PARTITIONING;

ALTER TABLE task_attribute_type
DROP PRIMARY KEY,
DROP COLUMN partition_date,
ADD PRIMARY KEY(id);

ALTER TABLE task_attribute_type
ADD CONSTRAINT unique_names UNIQUE(name);