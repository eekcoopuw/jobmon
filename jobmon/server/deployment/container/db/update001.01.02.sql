use `docker`;

ALTER TABLE `job_instance`
ADD COLUMN `maxpss` varchar(50) DEFAULT NULL;

LOCK TABLES `job_instance_status` WRITE;
INSERT INTO `job_instance_status` VALUES ('K','KILL_SELF');
UNLOCK TABLES;
