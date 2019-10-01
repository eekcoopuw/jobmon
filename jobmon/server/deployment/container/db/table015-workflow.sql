--
-- Table structure for table `workflow`
--
use `docker`;

DROP TABLE IF EXISTS `workflow`;
CREATE TABLE `workflow` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dag_id` int(11) DEFAULT NULL,
  `workflow_args` text,
  `workflow_hash` text,
  `description` text,
  `name` varchar(150) DEFAULT NULL,
  `user` varchar(150) DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `dag_id` (`dag_id`),
  KEY `status` (`status`),
  CONSTRAINT `workflow_ibfk_1` FOREIGN KEY (`dag_id`) REFERENCES `task_dag` (`dag_id`),
  CONSTRAINT `workflow_ibfk_2` FOREIGN KEY (`status`) REFERENCES `workflow_status` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
