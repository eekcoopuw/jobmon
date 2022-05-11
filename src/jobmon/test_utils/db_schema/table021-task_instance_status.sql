--
-- Table structure for table `task_instance_status`
--

use `docker`;

DROP TABLE IF EXISTS `task_instance_status`;

CREATE TABLE `task_instance_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOCK TABLES `task_instance_status` WRITE;
INSERT INTO `task_instance_status` VALUES ('D','DONE'),('E','ERROR'),('F','ERROR_FATAL'),('I','INSTANTIATED'),
('O','LAUNCHED'),('R','RUNNING'),('U','UNKNOWN_ERROR'),('W','NO_DISTRIBUTOR_ID'),('Z','RESOURCE_ERROR'),
('K','KILL_SELF'),('Q','QUEUED');
UNLOCK TABLES;
