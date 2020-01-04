
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
INSERT INTO `workflow_status` VALUES ('G', 'REGISTERED'),('B', 'BOUND'),('A', 'ABORTED'),('C','CREATED'),('D','DONE'),('F','FAILED'),('R','RUNNING');
UNLOCK TABLES;
