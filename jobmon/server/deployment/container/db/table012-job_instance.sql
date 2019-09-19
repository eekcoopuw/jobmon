--
-- Table structure for table `job_instance`
--
use `docker`;

DROP TABLE IF EXISTS `job_instance`;
CREATE TABLE `job_instance`(
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
  `submitted_date_short` date,
  PRIMARY KEY (`job_instance_id`, `submitted_date_short`),
  KEY `ix_job_instance_dag_id` (`dag_id`),
  KEY `ix_job_instance_executor_id` (`executor_id`),
  KEY `executor_parameter_set_id` (`executor_parameter_set_id`),
  KEY `status` (`status`),
  KEY `job_id` (`job_id`)
  ) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (TO_DAYS(submitted_date_short))
( PARTITION p201908 VALUES LESS THAN (TO_DAYS('2019-09-01'))ENGINE = InnoDB,
PARTITION p201909 VALUES LESS THAN (TO_DAYS('2019-10-01'))ENGINE = InnoDB,
PARTITION p201910 VALUES LESS THAN (TO_DAYS('2019-11-01'))ENGINE = InnoDB,
PARTITION p201911 VALUES LESS THAN (TO_DAYS('2019-12-01'))ENGINE = InnoDB,
PARTITION p201912 VALUES LESS THAN (TO_DAYS('2020-01-01'))ENGINE = InnoDB,
PARTITION p202001 VALUES LESS THAN (TO_DAYS('2020-02-01'))ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB

)*/;