--
-- Table structure for table `task_instance_status`
--

use `docker`;

DROP TABLE IF EXISTS `task_instance_status`;

CREATE TABLE `task_instance_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `task_instance_status` WRITE;
INSERT INTO `task_instance_status` VALUES ('B','SUBMITTED_TO_BATCH_EXECUTOR'),('D','DONE'),('E','ERROR'),('I','INSTANTIATED'),('R','RUNNING'),('U','UNKNOWN_ERROR'),('W','NO_EXECUTOR_ID'),('Z','RESOURCE_ERROR');
UNLOCK TABLES;
