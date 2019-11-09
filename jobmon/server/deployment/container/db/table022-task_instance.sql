--
-- Table structure for table `task_instance`
--
use `docker`;

DROP TABLE IF EXISTS `task_instance`;
CREATE TABLE `task_instance`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `workflow_run_id` INTEGER DEFAULT NULL,
  `executor_type` varchar(50) DEFAULT NULL,
  `executor_id` INTEGER DEFAULT NULL,
  `task_id` INTEGER NOT NULL,
  `executor_parameter_set_id` INTEGER DEFAULT NULL,
  `nodename` varchar(150) DEFAULT NULL,
  `process_group_id` INTEGER DEFAULT NULL,
  `usage_str` varchar(250) DEFAULT NULL,
  `wallclock` varchar(50) DEFAULT NULL,
  `maxrss` varchar(50) DEFAULT NULL,
  `maxpss` varchar(50) DEFAULT NULL,
  `cpu` varchar(50) DEFAULT NULL,
  `io` varchar(50) DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `submitted_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `report_by_date` datetime DEFAULT NULL,
  `partition_date` timestamp NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (`id`, `partition_date`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (UNIX_TIMESTAMP(partition_date))
( PARTITION p201908 VALUES LESS THAN (UNIX_TIMESTAMP('2019-09-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201909 VALUES LESS THAN (UNIX_TIMESTAMP('2019-10-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201910 VALUES LESS THAN (UNIX_TIMESTAMP('2019-11-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201911 VALUES LESS THAN (UNIX_TIMESTAMP('2019-12-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201912 VALUES LESS THAN (UNIX_TIMESTAMP('2020-01-01 00:00:00'))ENGINE = InnoDB,
PARTITION p202001 VALUES LESS THAN (UNIX_TIMESTAMP('2020-02-01 00:00:00'))ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB

)*/;

ALTER TABLE `task_instance` ADD INDEX `ix_workflow_run_id` (`workflow_run_id`);
ALTER TABLE `task_instance` ADD INDEX `ix_executor_id` (`executor_id`);
ALTER TABLE `task_instance` ADD INDEX `ix_executor_parameter_set_id` (`executor_parameter_set_id`);
ALTER TABLE `task_instance` ADD INDEX `ix_status` (`status`);
ALTER TABLE `task_instance` ADD INDEX `ix_task_id` (`task_id`);
