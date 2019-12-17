use `docker`;

LOCK TABLES `job_instance_status` WRITE;
INSERT INTO `job_instance_status` VALUES ('K','KILL_SELF');
UNLOCK TABLES;

LOCK TABLES `workflow_run_status` WRITE;
INSERT INTO `workflow_run_status` VALUES ('C','COLD_RESUME'),('H', 'HOT RESUME');
UNLOCK TABLES;