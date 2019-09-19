--
-- Table structure for table `job_status`
--
use `docker`;

DROP TABLE IF EXISTS `job_status`;
CREATE TABLE `job_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

LOCK TABLES `job_status` WRITE;
INSERT INTO `job_status` VALUES ('A','ADJUSTING_RESOURCES'),('D','DONE'),('E','ERROR_RECOVERABLE'),('F','ERROR_FATAL'),('G','REGISTERED'),('I','INSTANTIATED'),('Q','QUEUED_FOR_INSTANTIATION'),('R','RUNNING');
UNLOCK TABLES;


