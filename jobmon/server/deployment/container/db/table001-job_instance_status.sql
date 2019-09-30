--
-- Table structure for table `job_instance_status`
--

use `docker`;

DROP TABLE IF EXISTS `job_instance_status`;
CREATE TABLE `job_instance_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

LOCK TABLES `job_instance_status` WRITE;
INSERT INTO `job_instance_status` VALUES ('B','SUBMITTED_TO_BATCH_EXECUTOR'),('D','DONE'),('E','ERROR'),('I','INSTANTIATED'),('R','RUNNING'),('U','UNKNOWN_ERROR'),('W','NO_EXECUTOR_ID'),('Z','RESOURCE_ERROR');
UNLOCK TABLES;