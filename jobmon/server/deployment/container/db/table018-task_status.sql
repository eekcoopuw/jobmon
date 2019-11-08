--
-- Table structure for table `task_status`
--
use `docker`;

DROP TABLE IF EXISTS `task_status`;
CREATE TABLE `task_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `task_status` WRITE;
INSERT INTO `task_status` VALUES ('A','ADJUSTING_RESOURCES'),('D','DONE'),('E','ERROR_RECOVERABLE'),('F','ERROR_FATAL'),('G','REGISTERED'),('I','INSTANTIATED'),('Q','QUEUED_FOR_INSTANTIATION'),('R','RUNNING');
UNLOCK TABLES;


