
--
-- Table structure for table `workflow_status`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_status`;

CREATE TABLE `workflow_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `workflow_status` WRITE;
INSERT INTO `workflow_status` VALUES ('C','CREATED'),('D','DONE'),('E','ERROR'),('R','RUNNING'),('S','STOPPED');
UNLOCK TABLES;
