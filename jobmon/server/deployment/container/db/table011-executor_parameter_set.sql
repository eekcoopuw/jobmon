--
-- Table structure for table `executor_parameter_set`
--
use `docker`;

DROP TABLE IF EXISTS `executor_parameter_set`;
CREATE TABLE `executor_parameter_set` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_id` int(11) NOT NULL,
  `parameter_set_type` varchar(1) NOT NULL,
  `max_runtime_seconds` int(11) DEFAULT NULL,
  `context_args` varchar(1000) DEFAULT NULL,
  `resource_scales` varchar(1000) DEFAULT NULL,
  `queue` varchar(255) DEFAULT NULL,
  `num_cores` int(11) DEFAULT NULL,
  `m_mem_free` float DEFAULT NULL,
  `j_resource` tinyint(1) DEFAULT NULL,
  `hard_limits` tinyint(1) DEFAULT NULL,
  `partition_date` timestamp NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (`id`, `partition_date`),
  KEY `parameter_set_type` (`parameter_set_type`),
  KEY `job_id` (`job_id`)
  ) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (UNIX_TIMESTAMP(partition_date))
( PARTITION p201908 VALUES LESS THAN (UNIX_TIMESTAMP('2019-09-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201909 VALUES LESS THAN (UNIX_TIMESTAMP('2019-10-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201910 VALUES LESS THAN (UNIX_TIMESTAMP('2019-11-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201911 VALUES LESS THAN (UNIX_TIMESTAMP('2019-12-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201912 VALUES LESS THAN (UNIX_TIMESTAMP('2020-01-01 00:00:00'))ENGINE = InnoDB,
PARTITION p202001 VALUES LESS THAN (UNIX_TIMESTAMP('2020-02-01 00:00:00'))ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB

)*/;
