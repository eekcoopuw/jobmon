use `docker`;

ALTER TABLE `job_instance`
ADD COLUMN `maxpss` varchar(50) DEFAULT NULL;

LOCK TABLES `job_instance_status` WRITE;
INSERT INTO `job_instance_status` VALUES ('K','KILL_SELF');
UNLOCK TABLES;

ALTER TABLE `workflow_run_status`
MODIFY COLUMN id varchar(2);

LOCK TABLES `workflow_run_status` WRITE;
INSERT INTO `workflow_run_status` VALUES ('CR','COLD_RESUME'),('HR', 'HOT RESUME');
UNLOCK TABLES;