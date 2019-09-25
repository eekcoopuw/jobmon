--
-- Table structure for table `workflow_run`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_run`;
CREATE TABLE `workflow_run` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_id` int(11) DEFAULT NULL,
  `user` varchar(150) DEFAULT NULL,
  `hostname` varchar(150) DEFAULT NULL,
  `pid` int(11) DEFAULT NULL,
  `stderr` varchar(1000) DEFAULT NULL,
  `stdout` varchar(1000) DEFAULT NULL,
  `project` varchar(150) DEFAULT NULL,
  `working_dir` varchar(1000) DEFAULT NULL,
  `slack_channel` varchar(150) DEFAULT NULL,
  `executor_class` varchar(150) DEFAULT NULL,
  `resource_adjustment` float DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `workflow_id` (`workflow_id`),
  KEY `status` (`status`),
  CONSTRAINT `workflow_run_ibfk_1` FOREIGN KEY (`workflow_id`) REFERENCES `workflow` (`id`),
  CONSTRAINT `workflow_run_ibfk_2` FOREIGN KEY (`status`) REFERENCES `workflow_run_status` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=16891;
