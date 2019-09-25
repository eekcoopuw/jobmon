--
-- Table structure for table `task_dag`
--
use `docker`;

DROP TABLE IF EXISTS `task_dag`;
CREATE TABLE `task_dag` (
  `dag_id` int(11) NOT NULL AUTO_INCREMENT,
  `dag_hash` varchar(150) DEFAULT NULL,
  `name` varchar(150) DEFAULT NULL,
  `user` varchar(150) DEFAULT NULL,
  `heartbeat_date` datetime DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  PRIMARY KEY (`dag_id`)
) ENGINE=InnoDB;