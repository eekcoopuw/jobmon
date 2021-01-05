--
-- Table structure for table `workflow_run_status`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_run_status`;

CREATE TABLE `workflow_run_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `workflow_run_status` WRITE;
INSERT INTO `workflow_run_status` VALUES ('G', 'REGISTERED'),('B','BOUND'),('R','RUNNING'),('D','DONE'),('A','ABORTED'),('S','STOPPED'),('E','ERROR'),('C','COLD_RESUME'),('H','HOT_RESUME'),('T',"TERMINATED");
UNLOCK TABLES;
