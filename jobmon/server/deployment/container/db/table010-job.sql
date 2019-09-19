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
  `submitted_date_short` date,
  PRIMARY KEY (`job_id`, `submitted_date_short`),
  KEY `ix_dag_id_status_date` (`dag_id`,`status_date`),
  KEY `ix_job_status_date` (`status_date`),
  KEY `executor_parameter_set_id` (`executor_parameter_set_id`),
  KEY `status` (`status`)
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
