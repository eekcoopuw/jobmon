--
-- Table structure for table `workflow`
--
use `docker`;

DROP TABLE IF EXISTS `workflow`;

CREATE TABLE `workflow` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `tool_version_id` INTEGER DEFAULT NULL,
  `dag_id` INTEGER DEFAULT NULL,
  `workflow_args_hash` varchar(150),
  `task_hash` varchar(150),
  `description` varchar(1000),
  `name` varchar(150) DEFAULT NULL,
  `user` varchar(150) DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `heartbeat_date` datetime DEFAULT NULL,
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

ALTER TABLE `workflow` ADD INDEX `ix_tool_version_id` (`tool_version_id`);
ALTER TABLE `workflow` ADD INDEX `ix_dag_id` (`dag_id`);
ALTER TABLE `workflow` ADD INDEX `ix_status` (`status`);
