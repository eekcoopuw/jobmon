--
-- Table structure for table `job_instance`
--
use `docker`;

DROP TABLE IF EXISTS `job_instance`;
CREATE TABLE `job_instance` (
  `job_instance_id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_run_id` int(11) DEFAULT NULL,
  `executor_type` varchar(50) DEFAULT NULL,
  `executor_id` int(11) DEFAULT NULL,
  `job_id` int(11) NOT NULL,
  `dag_id` int(11) NOT NULL,
  `executor_parameter_set_id` int(11) NOT NULL,
  `usage_str` varchar(250) DEFAULT NULL,
  `nodename` varchar(50) DEFAULT NULL,
  `process_group_id` int(11) DEFAULT NULL,
  `wallclock` varchar(50) DEFAULT NULL,
  `maxrss` varchar(50) DEFAULT NULL,
  `cpu` varchar(50) DEFAULT NULL,
  `io` varchar(50) DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `submitted_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `report_by_date` datetime DEFAULT NULL,
  PRIMARY KEY (`job_instance_id`),
  KEY `ix_job_instance_dag_id` (`dag_id`),
  KEY `ix_job_instance_executor_id` (`executor_id`),
  KEY `executor_parameter_set_id` (`executor_parameter_set_id`),
  KEY `status` (`status`),
  KEY `job_id` (`job_id`),
  CONSTRAINT `job_instance_ibfk_1` FOREIGN KEY (`dag_id`) REFERENCES `task_dag` (`dag_id`),
  CONSTRAINT `job_instance_ibfk_2` FOREIGN KEY (`executor_parameter_set_id`) REFERENCES `executor_parameter_set` (`id`),
  CONSTRAINT `job_instance_ibfk_3` FOREIGN KEY (`status`) REFERENCES `job_instance_status` (`id`),
  CONSTRAINT `job_instance_ibfk_4` FOREIGN KEY (`job_id`) REFERENCES `job` (`job_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1804346;