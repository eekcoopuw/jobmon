--
-- Table structure for table `job`
--
use `docker`;

DROP TABLE IF EXISTS `job`;
CREATE TABLE `job` (
  `job_id` int(11) NOT NULL AUTO_INCREMENT,
  `dag_id` int(11) DEFAULT NULL,
  `job_hash` varchar(255) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `tag` varchar(255) DEFAULT NULL,
  `command` text,
  `executor_parameter_set_id` int(11) DEFAULT NULL,
  `num_attempts` int(11) DEFAULT NULL,
  `max_attempts` int(11) DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `submitted_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  PRIMARY KEY (`job_id`),
  KEY `ix_dag_id_status_date` (`dag_id`,`status_date`),
  KEY `ix_job_status_date` (`status_date`),
  KEY `executor_parameter_set_id` (`executor_parameter_set_id`),
  KEY `status` (`status`),
  CONSTRAINT `job_ibfk_2` FOREIGN KEY (`status`) REFERENCES `job_status` (`id`),
  CONSTRAINT `job_ibfk_3` FOREIGN KEY (`dag_id`) REFERENCES `task_dag` (`dag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;