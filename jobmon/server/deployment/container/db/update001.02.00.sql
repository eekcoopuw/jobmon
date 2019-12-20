use `docker`;

LOCK TABLES `workflow_run_attribute_type` WRITE;
Delete FROM `workflow_run_attribute_type` WHERE id IN (13, 14);
UNLOCK TABLES;