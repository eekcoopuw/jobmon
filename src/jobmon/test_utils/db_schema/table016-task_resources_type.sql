--
-- Table structure for table `task_resources_type`
--
use `docker`;

DROP TABLE IF EXISTS `task_resources_type`;

CREATE TABLE `task_resources_type` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOCK TABLES `task_resources_type` WRITE;
INSERT INTO `task_resources_type` VALUES ('A','ADJUSTED'),('O','ORIGINAL'),('V','VALIDATED');
UNLOCK TABLES;
