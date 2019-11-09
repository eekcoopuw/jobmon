--
-- Table structure for table `task`
--
use `docker`;

DROP TABLE IF EXISTS `task`;

CREATE TABLE `task` (
  `task_id` INTEGER NOT NULL AUTO_INCREMENT,
  `workflow_id` INTEGER DEFAULT NULL,
  `node_id` INTEGER DEFAULT NULL,
  `task_arg_hash` varchar(150) NOT NULL,
  `name` varchar(250) DEFAULT NULL,
  `command` text,
  `executor_parameter_set_id` INTEGER DEFAULT NULL,
  `num_attempts` INTEGER DEFAULT NULL,
  `max_attempts` INTEGER DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `submitted_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
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


ALTER TABLE `task` ADD INDEX `ix_workflow_id_status_date` (`workflow_id`,`status_date`);
ALTER TABLE `task` ADD INDEX `ix_status_date` (`status_date`);
ALTER TABLE `task` ADD INDEX `ix_executor_parameter_set_id` (`executor_parameter_set_id`);
ALTER TABLE `task` ADD INDEX `ix_status` (`status`);
